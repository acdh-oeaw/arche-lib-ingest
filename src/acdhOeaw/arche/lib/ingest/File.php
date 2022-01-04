<?php

/*
 * The MIT License
 *
 * Copyright 2021 Austrian Centre for Digital Humanities.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace acdhOeaw\arche\lib\ingest;

use DateTime;
use RuntimeException;
use SplFileInfo;
use EasyRdf\Resource;
use GuzzleHttp\Promise\PromiseInterface;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\ingest\SkippedException;
use acdhOeaw\arche\lib\ingest\util\ProgressMeter;
use acdhOeaw\arche\lib\ingest\util\UUID;
use acdhOeaw\arche\lib\promise\RepoResourcePromise;

/**
 * Description of File
 *
 * @author zozlak
 */
class File {

    private SplFileInfo $info;
    private Resource $meta;
    private Repo $repo;
    private ?int $n;
    private int $sizeLimit;
    private int $skipMode;
    private int $versioning;
    private bool $pidPass;
    private ?string $meterId;
    private RepoResource $repoRes;

    public function __construct(SplFileInfo $fileInfo, Resource $meta,
                                Repo $repo) {
        $this->info = $fileInfo;
        $this->meta = $meta;
        $this->repo = $repo;
    }

    /**
     * Synchronous version of the `uploadAsync()`.
     * 
     * @param int $sizeLimit
     * @param int $skipMode
     * @param int $versioning
     * @param string|null $meterId
     * @return RepoResource | SkippedException
     */
    public function upload(int $sizeLimit = -1,
                           int $skipMode = Indexer::SKIP_NONE,
                           int $versioning = Indexer::VERSIONING_NONE,
                           bool $pidPass = false, ?string $meterId = null): RepoResource | SkippedException {
        return $this->uploadAsync($sizeLimit, $skipMode, $versioning, $pidPass, $meterId = null)->wait();
    }

    /**
     * Uploads the file into the repository.
     * 
     * @param int $sizeLimit
     * @param int $skipMode skip rules based on existence of corresponding
     *   repository resource. One of Indexer::SKIP_NONE, Indexer::SKIP_EXIST,
     *   Indexer::SKIP_BINARY_EXIST, Indexer::SKIP_NOT_EXIST
     * @param int $versioning versioning mode - one of Indexer::VERSIONING_NONE,
     *   Indexer::VERSIONING_DATE, Indexer::VERSIONING_DIGEST, 
     *   Indexer::VERSIONING_ALWAYS
     * @param ?string $meterId identifier of the progress meter (if null, no
     *   progress information is displayed)
     * @return PromiseInterface
     */
    public function uploadAsync(int $sizeLimit = -1,
                                int $skipMode = Indexer::SKIP_NONE,
                                int $versioning = Indexer::VERSIONING_NONE,
                                bool $pidPass = false, ?string $meterId = null): PromiseInterface {
        // to make it easy to populate the whole required context trough promises
        $this->n          = ProgressMeter::increment($meterId);
        $this->sizeLimit  = $sizeLimit;
        $this->skipMode   = $skipMode;
        $this->versioning = $this->info->isDir() ? Indexer::VERSIONING_NONE : $versioning;
        $this->pidPass    = $pidPass;
        $this->meterId    = $meterId;

        $promise = $this->repo->getResourceByIdsAsync($this->getIds());
        $promise = $promise->then(function (RepoResource $repoRes) {
            $skip = $this->skipMode === Indexer::SKIP_EXIST || ($this->skipMode === Indexer::SKIP_BINARY_EXIST && $repoRes->hasBinaryContent());
            if ($skip) {
                echo ProgressMeter::format($this->meterId, $this->n, "Processing " . $this->info->getPathname() . " ({n}/{t} {p}%): skip\n");
                return new SkippedException();
            }
            $this->repoRes = $repoRes;
            if ($this->versioning !== Indexer::VERSIONING_NONE) {
                return $this->versioningAsync();
            }
            return $this->updateAsync();
        });
        $promise = $promise->otherwise(function ($error) {
            if (!($error instanceof NotFound)) {
                throw $error;
            }
            if ($this->skipMode === Indexer::SKIP_NOT_EXIST) {
                return new SkippedException();
            }
            return $this->createAsync();
        });
        return $promise;
    }

    public function getPath(): string {
        return $this->info->getPathname();
    }

    private function versioningAsync(): RepoResourcePromise | RepoResource {
        $schema = $this->repo->getSchema();

        // check if new version is needed
        $oldMeta = $this->repoRes->getMetadata();
        switch ($this->versioning) {
            case Indexer::VERSIONING_DATE:
                $modDate    = (string) $oldMeta->getLiteral($schema->modificationDate);
                $locModDate = (new DateTime())->setTimestamp($this->info->getMTime())->format('Y-m-d\TH:i:s');
                $newVersion = $locModDate > $modDate;
                break;
            case Indexer::VERSIONING_DIGEST:
                $hash       = explode(':', (string) $oldMeta->getLiteral($schema->hash));
                $locHash    = $this->getHash($hash[0]);
                $newVersion = $hash[1] !== $locHash;
                break;
            case Indexer::VERSIONING_ALWAYS:
                $newVersion = true;
                break;
            default:
                throw new RuntimeException("Unknown versioning mode $this->versioning");
        }
        if (!$newVersion) {
            return $this->updateAsync(true);
        }

        // progress meter
        $upload = $this->withinSizeLimit() ? '+ upload ' : '';
        echo ProgressMeter::format($this->meterId, $this->n, "Processing " . $this->info->getPathname() . " ({n}/{t} {p}%): new version $upload\n");

        // create the new version
        $repoIdNmsp = $this->repo->getBaseUrl();
        $skipProp   = [$schema->id];
        if (!$this->pidPass) {
            $skipProp[] = $schema->pid;
        }

        $newMeta = $oldMeta->copy($skipProp);
        $newMeta->addResource($schema->isNewVersionOf, $this->repoRes->getUri());
        if ($this->pidPass) {
            $oldMeta->deleteResource($schema->pid);
        }

        $idSkip = [];
        if (!$this->pidPass) {
            foreach ($oldMeta->allResources($schema->pid) as $pid) {
                $idSkip[] = (string) $pid;
            }
        }
        foreach ($oldMeta->allResources($schema->id) as $id) {
            $id = (string) $id;
            if (!in_array($id, $idSkip) && !str_starts_with($id, $repoIdNmsp)) {
                $newMeta->addResource($schema->id, $id);
                $oldMeta->deleteResource($schema->id, $id);
            }
        }
        $oldMeta->deleteResource($schema->parent);
        // there is at least one non-internal id required; as all are being passed to the new resource, let's create a dummy one
        $oldMeta->addResource($schema->id, $schema->namespaces->vid . UUID::v4());

        $this->meta = $newMeta;
        $oldRepoRes = $this->repoRes;
        $oldRepoRes->setMetadata($oldMeta);
        $promise    = $oldRepoRes->updateMetadataAsync(RepoResource::UPDATE_OVERWRITE, RepoResource::META_RESOURCE);
        if ($promise === null) {
            return $this->createAsync();
        }
        return new RepoResourcePromise($promise->then(fn() => $this->createAsync()));
    }

    private function updateAsync(bool $skipUpload = false): RepoResourcePromise | RepoResource {
        $binary = $this->getBinaryData($skipUpload);
        $upload = $binary !== null ? '+ upload ' : '';
        echo ProgressMeter::format($this->meterId, $this->n, "Processing " . $this->info->getPathname() . " ({n}/{t} {p}%): update $upload " . $this->repoRes->getUri() . "\n");

        if ($binary !== null) {
            $promise = $this->repoRes->updateContentAsync($binary);
            $promise = $promise->then(function () {
                $this->repoRes->setMetadata($this->meta);
                return $this->repoRes->updateMetadataAsync(RepoResource::UPDATE_MERGE, RepoResource::META_RESOURCE);
            });
        } else {
            $this->repoRes->setMetadata($this->meta);
            $promise = $this->repoRes->updateMetadataAsync(RepoResource::UPDATE_MERGE, RepoResource::META_RESOURCE);
        }
        if ($promise === null) {
            return $this->repoRes;
        }
        return new RepoResourcePromise($promise->then(fn() => $this->repoRes));
    }

    private function createAsync(): RepoResourcePromise {
        $upload = $this->withinSizeLimit() ? '+ upload ' : '';
        echo ProgressMeter::format($this->meterId, $this->n, "Processing " . $this->info->getPathname() . " ({n}/{t} {p}%): create $upload\n");

        $this->assureLabel();
        return $this->repo->createResourceAsync($this->meta, $this->getBinaryData());
    }

    /**
     * Returns acdhOeaw\arche\lib\BinaryPayload object representing file 
     * contents.
     * 
     * @return ?BinaryPayload
     */
    private function getBinaryData(bool $skip = false): ?BinaryPayload {
        if ($this->info->isDir() || !$this->withinSizeLimit() || $skip) {
            return null;
        }
        return new BinaryPayload(null, $this->info->getPathname());
    }

    private function getHash(string $hashName): string {
        $hash = hash_init($hashName);
        hash_update_file($hash, $this->info->getPathname());
        return hash_final($hash, false);
    }

    /**
     * 
     * @return array<string>
     */
    private function getIds(): array {
        $idProp = $this->repo->getSchema()->id;
        $ids    = [];
        foreach ($this->meta->allResources($idProp) as $id) {
            $ids[] = (string) $id;
        }
        return $ids;
    }

    private function withinSizeLimit(): bool {
        return $this->sizeLimit === -1 || $this->info->getSize() < $this->sizeLimit;
    }

    /**
     * Makes sure metadata will contain a label but if remote metadata label 
     * exists, it won't be overwritten.
     * @return void
     */
    private function assureLabel(): void {
        $labelProp = $this->repo->getSchema()->label;

        $addLabel = true;
        if (isset($this->repoRes)) {
            $addLabel = $this->repoRes->getMetadata()->get($labelProp) === null;
        }
        if ($addLabel) {
            $this->meta->addLiteral($labelProp, $this->info->getFilename(), 'und');
        }
    }
}
