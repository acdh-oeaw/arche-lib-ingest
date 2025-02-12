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
use GuzzleHttp\Promise\PromiseInterface;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\QuadInterface;
use quickRdf\DataFactory as DF;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\ingest\SkippedException;
use acdhOeaw\arche\lib\ingest\util\ProgressMeter;
use acdhOeaw\arche\lib\promise\RepoResourcePromise;

/**
 * Description of File
 *
 * @author zozlak
 */
class File {

    private SplFileInfo $info;
    private DatasetNodeInterface $meta;
    private Repo $repo;
    private ?int $n = null;
    private int $sizeLimit;
    private int $skipMode;
    private int $versioning;

    /**
     * A callable with signature
     * `function(\rdfInterface\DatasetNodeInterface $resourceMeta, \acdhOeaw\arche\lib\Schema $repositoryMetaSchema): array{0: \rdfInterface\DatasetNodeInterface $oldVersionMeta, 1: \rdfInterface\DatasetNodeInterface $newVersionMeta}
     * generating new and old version metadata based on the current version metadata
     * 
     * @var callable $versioningMetaFunc
     */
    private $versioningMetaFunc;

    /**
     * A callable with signature
     * `function(\acdhOeaw\arche\lib\RepoResource $old, \acdhOeaw\arche\lib\RepoResource $old $new): void
     * fixing references to the old resource (or doing any other versioning-related
     * metadata processing which requires the new version resource to be already created)
     * 
     * @var callable|null
     */
    private $versioningRefFunc = null;
    private ?string $meterId;
    private RepoResource $repoRes;
    private int $uploadsCount  = 0;

    public function __construct(SplFileInfo $fileInfo,
                                DatasetNodeInterface $meta, Repo $repo) {
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
     * @param callable|null $versioningMetaFunc a callable with signature
     *   `function(\rdfInterface\DatasetNodeInterface $resourceMeta): array{0: \rdfInterface\DatasetNodeInterface $oldVersionMeta, 1: \rdfInterface\DatasetNodeInterface $newVersionMeta}
     *   generating new and old version metadata based on the current version metadata
     * @param callable|null $versioningRefFunc a callable with signature
     *   `function(\acdhOeaw\arche\lib\RepoResource $old, \acdhOeaw\arche\lib\RepoResource $old $new): void
     *   fixing references to the old resource (or doing any other versioning-related
     *   metadata processing which requires the new version resource to be already created)
     * @param string|null $meterId
     * @return RepoResource | SkippedException
     */
    public function upload(int $sizeLimit = -1,
                           int $skipMode = Indexer::SKIP_NONE,
                           int $versioning = Indexer::VERSIONING_NONE,
                           ?callable $versioningMetaFunc = null,
                           ?callable $versioningRefFunc = null,
                           ?string $meterId = null): RepoResource | SkippedException {
        return $this->uploadAsync($sizeLimit, $skipMode, $versioning, $versioningMetaFunc, $versioningRefFunc, $meterId = null)->wait();
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
     * @param callable $versioningMetaFunc a callable with signature
     *   `function(\rdfInterface\DatasetNodeInterface $resourceMeta, \acdhOeaw\arche\lib\Schema $repositoryMetaSchema): array{0: \rdfInterface\DatasetNodeInterface $oldVersionMeta, 1: \rdfInterface\DatasetNodeInterface $newVersionMeta}
     *   generating new and old version metadata based on the current version metadata
     * @param callable $versioningRefFunc a callable with signature
     *   `function(\acdhOeaw\arche\lib\RepoResource $old, \acdhOeaw\arche\lib\RepoResource $old $new): void
     *   fixing references to the old resource (or doing any other versioning-related
     *   metadata processing which requires the new version resource to be already created)
     * @param ?string $meterId identifier of the progress meter (if null, no
     *   progress information is displayed)
     * @return PromiseInterface
     */
    public function uploadAsync(int $sizeLimit = -1,
                                int $skipMode = Indexer::SKIP_NONE,
                                int $versioning = Indexer::VERSIONING_NONE,
                                ?callable $versioningMetaFunc = null,
                                ?callable $versioningRefFunc = null,
                                ?string $meterId = null): PromiseInterface {
        $this->uploadsCount++;
        // to make it easy to populate the whole required context trough promises
        $this->n          = ProgressMeter::increment($meterId);
        $this->sizeLimit  = $sizeLimit;
        $this->skipMode   = $skipMode;
        $this->versioning = $this->info->isDir() ? Indexer::VERSIONING_NONE : $versioning;
        $this->meterId    = $meterId;
        if ($versioning !== Indexer::VERSIONING_NONE && $versioningMetaFunc === null) {
            throw new IndexerException('$versioningMetaFunc has to be provided if an automatic versioning is used');
        }
        if ($versioningMetaFunc !== null) {
            $this->versioningMetaFunc = $versioningMetaFunc;
        }
        if ($versioningRefFunc !== null) {
            $this->versioningRefFunc = $versioningRefFunc;
        }

        $promise = $this->repo->getResourceByIdsAsync($this->getIds());
        $promise = $promise->then(function (RepoResource $repoRes) {
            $skip = ($this->skipMode & Indexer::SKIP_EXIST) || (($this->skipMode & Indexer::SKIP_BINARY_EXIST) && $repoRes->hasBinaryContent());
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
            if ($this->skipMode & Indexer::SKIP_NOT_EXIST) {
                echo ProgressMeter::format($this->meterId, $this->n, "Processing " . $this->info->getPathname() . " ({n}/{t} {p}%): skip\n");
                return new SkippedException();
            }
            return $this->createAsync();
        });
        return $promise;
    }

    public function getPath(): string {
        return $this->info->getPathname();
    }

    public function getUploadsCount(): int {
        return $this->uploadsCount;
    }

    private function versioningAsync(): RepoResourcePromise | RepoResource {
        $schema = $this->repo->getSchema();

        // check if new version is needed
        $skipUpload = true;
        $oldRes     = $this->repoRes;
        $oldMeta    = $oldRes->getGraph();
        switch ($this->versioning) {
            case Indexer::VERSIONING_DATE:
                $modDate    = (string) $oldMeta->getObject($schema->modificationDate);
                $locModDate = (new DateTime())->setTimestamp($this->info->getMTime())->format('Y-m-d\TH:i:s');
                $newVersion = $locModDate > $modDate;
                break;
            case Indexer::VERSIONING_DIGEST:
                $hash       = (string) $oldMeta->getObject(new PT($schema->hash));
                if (empty($hash)) {
                    $newVersion = false;
                    $skipUpload = false;
                } else {
                    $hash       = explode(':', $hash);
                    $locHash    = $this->getHash($hash[0]);
                    $newVersion = $hash[1] !== $locHash;
                }
                break;
            case Indexer::VERSIONING_ALWAYS:
                $newVersion = true;
                break;
            default:
                throw new RuntimeException("Unknown versioning mode $this->versioning");
        }
        if (!$newVersion) {
            return $this->updateAsync($skipUpload);
        }

        // progress meter
        $upload = $this->withinSizeLimit() ? '+ upload ' : '';
        echo ProgressMeter::format($this->meterId, $this->n, "Processing " . $this->info->getPathname() . " ({n}/{t} {p}%): new version $upload " . $this->repoRes->getUri() . "\n");

        // create the new version
        list($oldMeta, $newMeta) = ($this->versioningMetaFunc)($oldMeta, $this->repo->getSchema());

        $this->meta = $newMeta;
        $oldRepoRes = $this->repoRes;
        $oldRepoRes->setMetadata($oldMeta);
        $promise    = $oldRepoRes->updateMetadataAsync(RepoResource::UPDATE_OVERWRITE, RepoResource::META_RESOURCE);
        if ($promise === null) {
            $promise = $this->createAsync();
        } else {
            $promise = new RepoResourcePromise($promise->then(fn() => $this->createAsync()));
        }
        if ($this->versioningRefFunc !== null) {
            $fn      = $this->versioningRefFunc;
            $promise = new RepoResourcePromise($promise->then(function (RepoResource $newRes) use ($oldRes,
                                                                                                   $fn) {
                    $fn($oldRes, $newRes);
                    return $newRes;
                }));
        }
        return $promise;
    }

    private function updateAsync(bool $skipUpload = false): RepoResourcePromise | RepoResource {
        $binary = $this->getBinaryData($skipUpload);
        $upload = $binary !== null ? '+ upload ' : '';
        echo ProgressMeter::format($this->meterId, $this->n, "Processing " . $this->info->getPathname() . " ({n}/{t} {p}%): update $upload " . $this->repoRes->getUri() . "\n");

        if ($binary !== null) {
            $promise = $this->repoRes->updateContentAsync($binary);
            $promise = $promise->then(function ($x) {
                // update the metadata only if it provides any new information
                $localMeta = $this->meta->map(fn(QuadInterface $q) => $q->withSubject($this->repoRes->getUri()));
                if (count($localMeta->copyExcept($this->repoRes->getGraph())) === 0) {
                    return true;
                } else {
                    $this->repoRes->setMetadata($this->meta);
                    return $this->repoRes->updateMetadataAsync(RepoResource::UPDATE_MERGE, RepoResource::META_RESOURCE);
                }
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
        return $this->meta->listObjects(new PT($idProp))->getValues();
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
        $addLabel  = !isset($this->repoRes) || $this->repoRes->getMetadata()->none(new PT($labelProp));
        if ($addLabel) {
            $this->meta->add(DF::quadNoSubject($labelProp, DF::literal($this->info->getFilename(), 'und')));
        }
    }
}
