<?php

/*
 * The MIT License
 *
 * Copyright 2019 Austrian Centre for Digital Humanities.
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

namespace acdhOeaw\acdhRepoIngest\schema;

use EasyRdf\Resource;
use acdhOeaw\acdhRepoLib\BinaryPayload;
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\RepoResource;
use acdhOeaw\acdhRepoLib\exception\NotFound;
use acdhOeaw\acdhRepoLib\exception\NotInCache;
use acdhOeaw\acdhRepoIngest\util\Geonames;
use acdhOeaw\acdhRepoIngest\util\UUID;

/**
 * Basic class for representing real-world entities to be imported into 
 * the repository.
 *
 * @author zozlak
 */
abstract class SchemaObject {

    /**
     * Debug mode switch.
     * @var boolean 
     */
    static public $debug = false;

    /**
     * Repository resource representing given entity.
     * @var \acdhOeaw\acdhRepoLib\RepoResource
     */
    private $res;

    /**
     * Entity id.
     * @var string
     */
    private $id;

    /**
     * External metadata to be merged with automatically generated one.
     * @var \EasyRdf\Resource
     */
    private $metadata;

    /**
     * List of automaticaly generated metadata properties to be preserved while
     * merging with external metadata.
     * @var array
     * @see $metadata
     */
    private $metadataPreserve = array();

    /**
     * Allows to keep track of the corresponding repository resource state:
     * - null - unknown
     * - true - recent call to updateRms() created the repository resource
     * - false - repository resource already existed uppon last updateRms() call
     * @var bool
     */
    protected $created;

    /**
     * repository connection object.
     * @var \acdhOeaw\acdhRepoLib\Repo
     */
    protected $repo;

    /**
     * Creates an object representing a real-world entity.
     * 
     * @param \acdhOeaw\acdhRepoLib\Repo $repo repository connection object
     * @param string $id entity identifier (derived class-specific)
     */
    public function __construct(Repo $repo, string $id) {
        $this->repo = $repo;
        $this->id   = $id;
    }

    /**
     * Creates RDF metadata from the real-world entity stored in this object.
     */
    abstract public function getMetadata(): Resource;

    /**
     * Returns repository resource representing given real-world entity.
     * 
     * If it does not exist, it can be created.
     * 
     * @param bool $create should repository resource be created if it does not
     *   exist?
     * @param bool $uploadBinary should binary data of the real-world entity
     *   be uploaded uppon repository resource creation?
     * @return \acdhOeaw\acdhRepoLib\RepoResource
     */
    public function getResource(bool $create = true, bool $uploadBinary = true): RepoResource {
        if ($this->res === null) {
            try {
                $this->findResource(false, false);
            } catch (NotFound $e) {
                $this->updateRms($create, $uploadBinary);
            }
        }
        return $this->res;
    }

    /**
     * Returns primary id of the real-world entity stored in this object
     * (as it was set up in the object contructor).
     * 
     * Please do not confuse this id with the random internal ACDH repo id.
     * 
     * @return string
     */
    public function getId(): string {
        return $this->id;
    }

    /**
     * Returns all known ids
     * 
     * @return array list of all ids
     */
    public function getIds(): array {
        $ids  = array($this->id);
        $meta = $this->getMetadata();
        foreach ($meta->allResources($this->repo->getSchema()->id) as $id) {
            $ids[] = $id->getUri();
        }
        $ids = array_unique($ids);
        return $ids;
    }

    /**
     * Updates repository resource representing a real-world entity stored in
     * this object.
     * 
     * @param bool $create should repository resource be created if it does not
     *   exist?
     * @param bool $uploadBinary should binary data of the real-world entity
     *   be uploaded uppon repository resource creation?
     * @return \acdhOeaw\acdhRepoLib\RepoResource
     */
    public function updateRms(bool $create = true, bool $uploadBinary = true): RepoResource {
        $this->created = $this->findResource($create, $uploadBinary);

        // if it has just been created it would be a waste of time to update it
        if (!$this->created) {
            $meta = $this->getMetadata();
            if ($this->metadata) {
                $meta->merge($this->metadata, $this->metadataPreserve);
            }
            //$this->repo->fixMetadataReferences($meta);
            $meta = $this->mergeMetadata($this->res->getMetadata(), $meta);
            $this->res->setMetadata($meta);
            $this->res->updateMetadata();

            $binaryContent = $this->getBinaryData();
            if ($uploadBinary && $binaryContent !== '') {
                $this->res->updateContent($binaryContent, true);
            }
        }

        return $this->res;
    }

    /**
     * Informs about the corresponding repository resource state uppon last call
     * to the `updateRms()` method:
     * - null - the updateRms() was not called yet
     * - true - repository resource was created by last call to the updateRms()
     * - false - repository resource already existed uppoin last call to the
     *   updateRms()
     * @return bool
     */
    public function getCreated(): bool {
        return $this->created;
    }

    /**
     * Sets an external metadata to be appended to automatically generated ones.
     * 
     * If a given metatada property exists both in automatically generated and
     * provided metadata, then the final result depends on the $preserve parameter:
     * - if the property is listed in the $preserve array, both automatically
     *   generated and provided values will be kept
     * - if not, only values from provided metadata will be kept and automatically
     *   generated ones will be skipped
     * 
     * @param Resource $meta external metadata
     * @param array $preserve list of metadata properties to be kept - see above
     */
    public function setMetadata(Resource $meta, array $preserve = array()) {
        $this->metadata         = $meta;
        $this->metadataPreserve = $preserve;
    }

    /**
     * Creates a new version of the resource. The new version inherits all IDs but
     * the UUID and epic PIDs. The old version looses all IDs but the UUID and
     * spic PIDs. It also looses all schema::parent property connections with collections.
     * The old and the new resource are linked with `schema:isNewVersion`
     * and `schema:isOldVersion`.
     * 
     * @param bool $uploadBinary should binary data of the real-world entity
     *   be uploaded uppon repository resource creation?
     * @param string $path where to create a resource (if it does not exist).
     *   If it it ends with a "/", the resource will be created as a child of
     *   a given collection). All the parents in the repository resource tree have
     *   to exist (you can not create "/foo/bar" if "/foo" does not exist already).
     * @param bool $pidPass should PIDs (epic handles) be migrated to the new
     *   version (`true`) or kept by the old one (`false`)
     * @return \acdhOeaw\acdhRepoLib\RepoResource old version resource
     */
    public function createNewVersion(bool $uploadBinary = true,
                                     bool $pidPass = false): RepoResource {
        $pidProp       = $this->repo->getSchema()->ingest->epicPid;
        $idProp        = $this->repo->getSchema()->id;
        $relProp       = $this->repo->getSchema()->parent;
        $repoIdNmsp    = $this->repo->getBaseUrl();
        $vidNmsp       = $this->repo->getSchema()->ingest->vidNamespace;
        $isNewVerProp  = $this->repo->getSchema()->ingest->isNewVersion;
        $isPrevVerProp = $this->repo->getSchema()->ingest->isPrevVersion;
        $skipProp      = [$idProp];
        if (!$pidPass) {
            $skipProp[] = $pidProp;
        }

        $this->findResource(false, $uploadBinary);
        $oldMeta = $this->res->getMetadata(true);
        $newMeta = $oldMeta->copy($skipProp);
        $newMeta->addResource($isNewVerProp, $this->res->getUri());
        if ($pidPass) {
            $oldMeta->deleteResource($pidProp);
        }

        $idSkip = [];
        if (!$pidPass) {
            foreach ($oldMeta->allResources($pidProp) as $pid) {
                $idSkip[] = (string) $pid;
            }
        }
        foreach ($oldMeta->allResources($idProp) as $id) {
            $id = (string) $id;
            if (!in_array($id, $idSkip) && strpos($id, $repoIdNmsp) !== 0) {
                $newMeta->addResource($idProp, $id);
                $oldMeta->deleteResource($idProp, $id);
            }
        }
        $oldMeta->deleteResource($relProp);
        // there is at least one non-UUID ID required; as all are being passed to the new resource, let's create a dummy one
        $oldMeta->addResource($idProp, $vidNmsp . UUID::v4());

        $oldRes  = $this->res;
        $oldRes->setMetadata($oldMeta);
        $oldRes->updateMetadata(RepoResource::UPDATE_OVERWRITE);
        $oldMeta = $oldRes->getMetadata();

        $this->createResource($newMeta, $uploadBinary);

        $oldMeta->addResource($isPrevVerProp, $this->res->getUri());
        $oldRes->setMetadata($oldMeta);
        $oldRes->updateMetadata();

        return $oldRes;
    }

    /**
     * Tries to find a repository resource representing a given object.
     * 
     * @param bool $create should repository resource be created if it was not
     *   found?
     * @param bool $uploadBinary should binary data of the real-world entity
     *   be uploaded uppon repository resource creation?
     * @return boolean if a repository resource was found
     */
    protected function findResource(bool $create = true,
                                    bool $uploadBinary = true): bool {
        $ids    = $this->getIds();
        echo self::$debug ? "searching for " . implode(', ', $ids) . "\n" : "";
        $result = '';

        try {
            $this->res = $this->repo->getResourceByIds($ids);
            $result    = 'found in cache';
        } catch (NotFound $e) {
            if (!$create) {
                throw $e;
            }

            $meta   = $this->getMetadata();
            $this->createResource($meta, $uploadBinary);
            $result = 'not found - created';
        }

        echo self::$debug ? "\t" . $result . " - " . $this->res->getUri() . "\n" : "";
        return $result == 'not found - created';
    }

    /**
     * Creates a repository resource
     * @param Resource $meta
     * @param bool $uploadBinary
     * @param string $path
     */
    protected function createResource(Resource $meta, bool $uploadBinary): void {
        //$this->repo->fixMetadataReferences($meta, [$this->repo->getSchema()->ingest->epicPid]);
        Geonames::standardizeProperty($meta, $this->repo->getSchema()->id);
        $binary    = $uploadBinary ? $this->getBinaryData() : null;
        $this->res = $this->repo->createResource($meta, $binary);
    }

    /**
     * Provides entity binary data.
     * @return value accepted as the \acdhOeaw\acdhRepoLib\Repo::attachData() $body parameter
     */
    protected function getBinaryData(): ?BinaryPayload {
        return null;
    }

    /**
     * Merges metadata coming from the repository and generated by the class.
     * @param Resource $current current repository resource metadata
     * @param Resource $new metadata generated by the class
     * @return Resource final metadata
     */
    protected function mergeMetadata(Resource $current, Resource $new): Resource {
        $idProp = $this->repo->getSchema()->id;
        $meta   = $current->merge($new, [$idProp]);
        Geonames::standardizeMeta($meta, $idProp);
        return $meta;
    }

}
