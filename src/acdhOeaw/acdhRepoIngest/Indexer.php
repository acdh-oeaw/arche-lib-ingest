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

namespace acdhOeaw\acdhRepoIngest;

use BadMethodCallException;
use InvalidArgumentException;
use RuntimeException;
use DateTime;
use DirectoryIterator;
use acdhOeaw\UriNormalizer;
use acdhOeaw\acdhRepoLib\exception\NotFound;
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\RepoResource;
use acdhOeaw\acdhRepoIngest\metaLookup\MetaLookupInterface;
use acdhOeaw\acdhRepoIngest\metaLookup\MetaLookupException;
use acdhOeaw\acdhRepoIngest\schema\File;

/**
 * Ingests files into the repository
 *
 * @author zozlak
 */
class Indexer {

    const MATCH             = 1;
    const SKIP              = 2;
    const SKIP_NONE         = 1;
    const SKIP_NOT_EXIST    = 2;
    const SKIP_EXIST        = 3;
    const SKIP_BINARY_EXIST = 4;
    const VERSIONING_NONE   = 1;
    const VERSIONING_ALWAYS = 2;
    const VERSIONING_DIGEST = 3;
    const VERSIONING_DATE   = 4;
    const PID_KEEP          = 1;
    const PID_PASS          = 2;

    /**
     * Detected operating system path enconding.
     * @var string
     */
    static private $pathEncoding;

    /**
     * Extracts relative path from a full path (by skipping cfg:containerDir)
     * @param string $fullPath
     * @param string $containerDir
     */
    static public function getRelPath(string $fullPath, string $containerDir): string {
        if (!strpos($fullPath, $containerDir) === 0) {
            throw new InvalidArgumentException('path is outside the container');
        }
        $containerDir = preg_replace('|/$|', '', $containerDir);
        return substr($fullPath, strlen($containerDir) + 1);
    }

    /**
     * Tries to detect path encoding used in the operating system by looking
     * into locale settings.
     * @throws RuntimeException
     */
    static private function detectPathEncoding() {
        if (self::$pathEncoding) {
            return;
        }
        foreach (explode(';', setlocale(LC_ALL, 0)) as $i) {
            $i = explode('=', $i);
            if ($i[0] === 'LC_CTYPE') {
                $tmp = preg_replace('|^.*[.]|', '', $i[1]);
                if (is_numeric($tmp)) {
                    self::$pathEncoding = 'windows-' . $tmp;
                    break;
                } else if (preg_match('|utf-?8|i', $tmp)) {
                    self::$pathEncoding = 'utf-8';
                    break;
                } else {
                    throw new RuntimeException('Operation system encoding can not be determined');
                }
            }
        }
    }

    /**
     * Sanitizes file path - turns all \ into / and assures it is UTF-8 encoded.
     * @param string $path
     * @param string $pathEncoding
     * @return string
     */
    static public function sanitizePath(string $path,
                                        string $pathEncoding = null): string {
        if ($pathEncoding === null) {
            self::detectPathEncoding();
            $pathEncoding = self::$pathEncoding;
        }
        $path = iconv($pathEncoding, 'utf-8', $path);
        $path = str_replace('\\', '/', $path);
        return $path;
    }

    /**
     * Turns debug messages on
     * @var bool
     */
    static public $debug = false;

    /**
     * RepoResource which children are created by the Indexer
     * @var \acdhOeaw\acdhRepoLib\RepoResource
     */
    private $parent;

    /**
     * File system paths where resource children are located
     * 
     * It is a concatenation of the container root path coming from the
     * class settings and the location path
     * properties of the RepoResource set using the `setParent()` method.
     * 
     * They can be also set manually using the `setPaths()` method
     * 
     * @var array
     */
    private $paths = [''];

    /**
     * Regular expression for matching child resource file names.
     * @var string 
     */
    private $filter = '//';

    /**
     * Regular expression for excluding child resource file names.
     * @var string 
     */
    private $filterNot = '';

    /**
     * Should children be directly attached to the RepoResource or maybe
     * each subdirectory should result in a separate collection resource
     * containing its children.
     * @var bool
     */
    private $flatStructure = false;

    /**
     * Maximum size of a child resource (in bytes) resulting in the creation
     * of binary resources.
     * 
     * For child resources bigger then this limit an "RDF only" Fedora resources
     * will be created.
     * 
     * Special value of -1 means "import all no matter their size"
     * 
     * @var int
     */
    private $uploadSizeLimit = -1;

    /**
     * URI of an RDF class assigned to indexed collections.
     * @var string
     */
    private $collectionClass;

    /**
     * URI of an RDF class assigned to indexed binary resources.
     * @var type 
     */
    private $binaryClass;

    /**
     * How many subsequent subdirectories should be indexed.
     * 
     * @var int 
     */
    private $depth = 1000;

    /**
     * Number of resource automatically triggering a commit (0 - no auto commit)
     * @var int
     */
    private $autoCommit = 0;

    /**
     * Base ingestion path to be substituted with the $containerToUriPrefix
     * to form a binary id.
     * 
     * @var string
     */
    private $containerDir;

    /**
     * Namespaces to substitute the $containerDir in the ingested binary path
     * to form a binary id.
     * 
     * @var string
     */
    private $containerToUriPrefix;

    /**
     * Should resources be created for empty directories.
     * 
     * Skipped if `$flatStructure` equals to `true`
     * 
     * @var bool 
     */
    private $includeEmpty = false;

    /**
     * Should files (not)existing in the Fedora be skipped?
     * @see setSkip()
     * @var int
     */
    private $skipMode = self::SKIP_NONE;

    /**
     * Should new versions of binary resources already existing in the Fedora
     * be created (if not, an existing resource is simply overwritten).
     * 
     * @var int
     */
    private $versioningMode = self::VERSIONING_NONE;

    /**
     * Should PIDs (epic handles) be migrated to the new version of a resource
     * during versioning.
     * @var int
     */
    private $pidPass = self::PID_KEEP;

    /**
     * An object providing metadata when given a resource file path
     * @var \acdhOeaw\util\metaLookup\MetaLookupInterface
     */
    private $metaLookup;

    /**
     * Should files without external metadata (provided by the `$metaLookup`
     * object) be skipped.
     * @var bool
     */
    private $metaLookupRequire = false;

    /**
     * Repository connection
     * @var \acdhOeaw\acdhRepoLib\Repo
     */
    private $repo;

    /**
     * Collection of resources commited during the ingestion. Used to handle
     * errors.
     * @var array
     * @see index()
     */
    private $commitedRes;

    /**
     * Collection of indexed resources
     * @var array
     * @see index()
     */
    private $indexedRes;

    /**
     * 
     * @param string $containerDir
     * @param string $containerToUriPrefix
     */
    public function __construct(string $containerDir,
                                string $containerToUriPrefix) {
        $this->containerDir         = preg_replace('|/$|', '', $containerDir) . '/';
        $this->containerToUriPrefix = preg_replace('|/$|', '', $containerToUriPrefix) . '/';
    }

    /**
     * Sets the repository connection object
     * @param \acdhOeaw\acdhRepoLib\Repo
     */
    public function setRepo(Repo $repo): Indexer {
        $this->repo = $repo;
        $this->readRepoConfig();
        return $this;
    }

    /**
     * Sets the parent resource for the indexed files.
     * 
     * Automatically sets ingestion paths based on the resource's 
     * cfg.schema.ingest.location RDF property values (only existing directories
     * are set).
     * 
     * @param \acdhOeaw\acdhRepoLib\RepoResource $resource
     */
    public function setParent(RepoResource $resource): Indexer {
        $this->parent = $resource;
        $this->repo   = $this->parent->getRepo();
        $this->readRepoConfig();
        $metadata     = $this->parent->getMetadata();
        $locations    = $metadata->allLiterals($this->repo->getSchema()->ingest->location);
        $this->paths  = [];
        foreach ($locations as $i) {
            $loc = preg_replace('|/$|', '', $this->containerDir . $i->getValue());
            if (is_dir($loc)) {
                $this->paths[] = $i->getValue();
            }
        }
        return $this;
    }

    /**
     * Overrides file system paths to look into for child resources.
     * 
     * @param array $paths
     * @return \acdhOeaw\util\Indexer
     */
    public function setPaths(array $paths): Indexer {
        $this->paths = $paths;
        return $this;
    }

    /**
     * Controls the automatic commit behaviour.
     * 
     * Even when you use autocommit, you should commit your transaction after
     * `Indexer::index()` (the only exception is when you set auto commit to 1
     * forcing commiting each and every resource separately but you probably 
     * don't want to do that for performance reasons).
     * @param int $count number of resource automatically triggering a commit 
     *   (0 - no auto commit)
     * @return \acdhOeaw\util\Indexer
     */
    public function setAutoCommit(int $count): Indexer {
        $this->autoCommit = $count;
        return $this;
    }

    /**
     * Defines if (and how) resources should be skipped from indexing based on
     * their (not)existance in Fedora.
     * 
     * @param int $skipMode mode either Indexer::SKIP_NONE (default), 
     *   Indexer::SKIP_NOT_EXIST, Indexer::SKIP_EXIST or 
     *   Indexer::SKIP_BINARY_EXIST
     * @return \acdhOeaw\util\Indexer
     */
    public function setSkip(int $skipMode): Indexer {
        if (!in_array($skipMode, [self::SKIP_NONE, self::SKIP_NOT_EXIST, self::SKIP_EXIST,
                self::SKIP_BINARY_EXIST])) {
            throw new BadMethodCallException('Wrong skip mode');
        }
        $this->skipMode = $skipMode;
        return $this;
    }

    /**
     * Defines if new versions of binary resources should be created or if they
     * should be simply overwritten with a new binary payload.
     * 
     * @param int $versioningMode mode either Indexer::VERSIONING_NONE, 
     *   Indexer::VERSIONING_ALWAYS, Indexer::VERSIONING_CHECKSUM or 
     *   Indexer::VERSIONING_DATE
     * @param int $migratePid should PIDs (epic handles) be migrated to the new
     *   version - either Indexer::MIGRATE_NO or Indexer::MIGRATE_YES
     * @return \acdhOeaw\util\Indexer
     * @throws BadMethodCallException
     */
    public function setVersioning(int $versioningMode,
                                  int $migratePid = self::PID_KEEP): Indexer {
        if (!in_array($versioningMode, [self::VERSIONING_NONE, self::VERSIONING_ALWAYS,
                self::VERSIONING_DIGEST, self::VERSIONING_DATE])) {
            throw new BadMethodCallException('Wrong versioning mode');
        }
        $this->versioningMode = $versioningMode;
        $this->pidPass        = $migratePid;
        return $this;
    }

    /**
     * Sets default RDF class for imported collections.
     * 
     * Overrides setting read form the `cfg::defaultCollectionClass` 
     * configuration property.
     * @param string $class
     * @return \acdhOeaw\util\Indexer
     */
    public function setCollectionClass(string $class): Indexer {
        $this->collectionClass = $class;
        return $this;
    }

    /**
     * Sets default RDF class for imported binary resources.
     * 
     * Overrides setting read form the `cfg::defaultBinaryClass` 
     * configuration property.
     * @param string $class
     * @return \acdhOeaw\util\Indexer
     */
    public function setBinaryClass(string $class): Indexer {
        $this->binaryClass = $class;
        return $this;
    }

    /**
     * Sets file name filter for child resources.
     * 
     * You can choose if file names must match or must not match (skip) the 
     * filter using the $type parameter. You can set both match and skip
     * filters by calling setFilter() two times (once with 
     * `$type = Indexer::MATCH` and second time with `$type = Indexer::SKIP`).
     * 
     * Filter is applied only to file names but NOT to directory names.
     * 
     * @param string $filter regular expression conformant with preg_replace()
     * @param int $type decides if $filter is a match or skip filter (can be
     *   one of Indexer::MATCH and Indexer::SKIP)
     * @return \acdhOeaw\util\Indexer
     */
    public function setFilter(string $filter, int $type = self::MATCH): Indexer {
        switch ($type) {
            case self::MATCH:
                $this->filter    = $filter;
                break;
            case self::SKIP:
                $this->filterNot = $filter;
                break;
            default:
                throw new BadMethodCallException('wrong $type parameter');
        }
        return $this;
    }

    /**
     * Sets if child resources be directly attached to the indexed RepoResource
     * (`$ifFlat` equals to `true`) or a separate collection repository resource
     * be created for each subdirectory (`$ifFlat` equals to `false`).
     * 
     * @param bool $ifFlat
     * @return \acdhOeaw\util\Indexer
     */
    public function setFlatStructure(bool $ifFlat): Indexer {
        $this->flatStructure = $ifFlat;
        return $this;
    }

    /**
     * Sets size treshold for uploading child resources as binary resources.
     * 
     * For files bigger then this treshold a "pure RDF" Fedora resources will
     * be created containing full metadata but no binary content.
     * 
     * @param bool $limit maximum size in bytes; 0 will cause no files upload,
     *   special value of -1 (default) will cause all files to be uploaded no 
     *   matter their size
     * @return \acdhOeaw\util\Indexer
     */
    public function setUploadSizeLimit(int $limit): Indexer {
        $this->uploadSizeLimit = $limit;
        return $this;
    }

    /**
     * Sets maximum indexing depth. 
     * 
     * @param int $depth maximum indexing depth (0 - only initial Resource dir, 1 - also its direct subdirectories, etc.)
     * @return \acdhOeaw\util\Indexer
     */
    public function setDepth(int $depth): Indexer {
        $this->depth = $depth;
        return $this;
    }

    /**
     * Sets if Fedora resources should be created for empty directories.
     * 
     * Note this setting is skipped when the `$flatStructure` is set to `true`.
     * 
     * @param bool $include should resources be created for empty directories
     * @return \acdhOeaw\util\Indexer
     * @see setFlatStructure()
     */
    public function setIncludeEmptyDirs(bool $include): Indexer {
        $this->includeEmpty = $include;
        return $this;
    }

    /**
     * Sets up the ingestion container path. This path is replaced with the
     * $containerToUriPrefix to form a binary id.
     * @param string $containerDir container dir path
     * @return \acdhOeaw\acdhRepoIngest\Indexer
     */
    public function setContainerDir(string $containerDir): Indexer {
        $this->containerDir = $containerDir;
        return $this;
    }

    /**
     * Sets up the namespace replaceing the $containerDir to form a binary id.
     * @param string $prefix namespace
     * @return \acdhOeaw\acdhRepoIngest\Indexer
     */
    public function setContainerToUriPrefix(string $prefix): Indexer {
        $this->containerToUriPrefix = $prefix;
        return $this;
    }

    /**
     * Sets a class providing metadata for indexed files.
     * @param MetaLookupInterface $metaLookup
     * @param bool $require should files lacking external metadata be skipped
     * @return \acdhOeaw\util\Indexer
     */
    public function setMetaLookup(MetaLookupInterface $metaLookup,
                                  bool $require = false): Indexer {
        $this->metaLookup        = $metaLookup;
        $this->metaLookupRequire = $require;
        return $this;
    }

    /**
     * Performs the indexing.
     * @return array a list RepoResource objects representing indexed resources
     */
    public function index(): array {
        list($indexed, $commited) = $this->__index();

        $this->indexedRes  = [];
        $this->commitedRes = [];
        return $indexed;
    }

    /**
     * Performs the indexing.
     * @return array a two-element array with first element containing a collection
     *   of indexed resources and a second one containing a collection of commited
     *   resources
     */
    private function __index(): array {
        $this->indexedRes  = [];
        $this->commitedRes = [];

        if (count($this->paths) === 0) {
            throw new RuntimeException('No paths set');
        }

        try {
            foreach ($this->paths as $path) {
                foreach (new DirectoryIterator($this->containerDir . $path) as $i) {
                    $this->indexEntry($i);
                }
            }
        } catch (Throwable $e) {
            if ($e instanceof IndexerException) {
                $this->commitedRes = array_merge($this->commitedRes, $e->getCommitedResources());
            }
            throw new IndexerException($e->getMessage(), $e->getCode(), $e, $this->commitedRes);
        }

        return [$this->indexedRes, $this->commitedRes];
    }

    /**
     * Processes single directory entry
     * @param DirectoryIterator $i
     * @return array
     */
    private function indexEntry(DirectoryIterator $i) {
        if ($i->isDot()) {
            return;
        }

        echo self::$debug ? $i->getPathname() . "\n" : "";

        $skip   = $this->isSkipped($i);
        $upload = $i->isFile() && ($this->uploadSizeLimit > $i->getSize() || $this->uploadSizeLimit === -1);

        $skip2 = false; // to be able to recursively go into directory we can't reuse $skip
        if (!$skip) {
            $location = self::getRelPath(self::sanitizePath($i->getPathname()), $this->containerDir);
            $id       = $this->containerToUriPrefix . str_replace('%2F', '/', rawurlencode($location));
            $class    = $i->isDir() ? $this->collectionClass : $this->binaryClass;
            $parent   = $this->parent === null ? null : $this->parent->getUri();
            $file     = new File($this->repo, $id, $i->getPathname(), $location, $class, $parent);
            if ($this->metaLookup) {
                $file->setMetaLookup($this->metaLookup, $this->metaLookupRequire);
            }
            $skip2 = $this->isSkippedExisting($file);
        }
        if (!$skip && !$skip2) {
            try {
                $res                                 = $this->performUpdate($i, $file, $upload);
                $this->indexedRes[$i->getPathname()] = $res;
                $this->handleAutoCommit();
            } catch (MetaLookupException $e) {
                if ($this->metaLookupRequire) {
                    $skip = true;
                } else {
                    throw $e;
                }
            } catch (NotFound $e) {
                if ($this->skipMode === self::SKIP_NOT_EXIST) {
                    $skip = true;
                } else {
                    throw $e;
                }
            }
        } else if ($skip2) {
            $res = $file->getResource(false, false);
        }
        if ($skip || $skip2) {
            echo self::$debug ? "\tskip" . ($skip2 ? '2' : '') . "\n" : "";
        }

        echo self::$debug && !$skip && !$skip2 ? "\t" . $res->getUri() . "\n\t" . $res->getUri() . "\n" : "";

        // recursion
        if ($i->isDir() && (!$skip || $this->flatStructure && $this->depth > 0)) {
            echo self::$debug ? "entering " . $i->getPathname() . "\n" : "";
            $ind = clone($this);
            if (!$this->flatStructure) {
                $ind->parent = $res;
            }
            $ind->setDepth($this->depth - 1);
            $path              = self::getRelPath($i->getPathname(), $this->repo->getSchema()->ingest->containerDir);
            $ind->setPaths([$path]);
            list($recRes, $recCom) = $ind->__index();
            $this->indexedRes  = array_merge($this->indexedRes, $recRes);
            $this->commitedRes = array_merge($this->commitedRes, $recCom);
            echo self::$debug ? "going back from " . $path : "";
            $this->handleAutoCommit();
            echo self::$debug ? "\n" : "";
        }
    }

    /**
     * Checks if a given file should be skipped because it already exists in the
     * repository while the Indexer skip mode is set to SKIP_EXIST or SKIP_BINARY_EXIST.
     * @param File $file file to be checked
     * @return bool
     * @throws MetaLookupException
     */
    private function isSkippedExisting(File $file): bool {
        if (!in_array($this->skipMode, [self::SKIP_EXIST, self::SKIP_BINARY_EXIST])) {
            return false;
        }
        try {
            $res = $file->getResource(false, false);
            return $res->hasBinaryContent() || $this->skipMode === self::SKIP_EXIST;
        } catch (MetaLookupException $e) {
            if ($this->metaLookupRequire) {
                throw $e;
            } else {
                return true;
            }
        } catch (NotFound $e) {
            return false;
        }
    }

    /**
     * Checks if a given file system node should be skipped during import.
     * 
     * @param DirectoryIterator $i file system node
     * @return bool
     */
    private function isSkipped(DirectoryIterator $i): bool {
        $isEmptyDir = true;
        if ($i->isDir()) {
            foreach (new DirectoryIterator($i->getPathname()) as $j) {
                if (!$j->isDot()) {
                    $isEmptyDir = false;
                    break;
                }
            }
        }
        $skipDir         = (!$this->includeEmpty && ($this->depth == 0 || $isEmptyDir) || $this->flatStructure);
        $filenameInclude = preg_match($this->filter, $i->getFilename());
        $filenameExclude = strlen($this->filterNot) > 0 && preg_match($this->filterNot, $i->getFilename());
        $skipFile        = !$filenameInclude || $filenameExclude;
        return $i->isDir() && $skipDir || !$i->isDir() && $skipFile;
    }

    /**
     * Performs autocommit if needed
     * @return bool if autocommit was performed
     */
    private function handleAutoCommit(): bool {
        $diff = count($this->indexedRes) - count($this->commitedRes);
        if ($diff >= $this->autoCommit && $this->autoCommit > 0) {
            echo self::$debug ? " + autocommit" : '';
            $this->repo->commit();
            $this->commitedRes = $this->indexedRes;
            $this->repo->begin();
            return true;
        }
        return false;
    }

    /**
     * Performs file upload taking care of versioning.
     * @param DirectoryIterator $iter
     * @param File $file
     * @param string $parent
     * @param bool $upload
     * @return \acdhOeaw\acdhRepoLib\RepoResource
     */
    public function performUpdate(DirectoryIterator $iter, File $file,
                                  bool $upload): RepoResource {
        // check versioning conditions
        $versioning = $this->versioningMode !== self::VERSIONING_NONE && !$iter->isDir();
        if ($versioning) {
            try {
                $res  = $file->getResource(false);
                $meta = $res->getMetadata();
                switch ($this->versioningMode) {
                    case self::VERSIONING_DATE:
                        $modDate    = (string) $meta->getLiteral($this->repo->getSchema()->modificationDate);
                        $locModDate = (new DateTime())->setTimestamp($iter->getMTime())->format('Y-m-d\TH:i:s');
                        $versioning = $locModDate > $modDate;
                        break;
                    case self::VERSIONING_DIGEST:
                        $hash       = explode(':', (string) $meta->getLiteral($this->repo->getSchema()->hash));
                        $locHash    = $this->getFileHash($iter->getPathname(), $hash[0]);
                        $versioning = $hash[1] !== $locHash;
                        break;
                    case self::VERSIONING_ALWAYS:
                        $versioning = true;
                        break;
                }
                // it we decided they are same it makes no sense to upload
                $upload = $versioning;
            } catch (NotFound $ex) {
                $versioning = false;
            }
        }

        $skipNotExist = $this->skipMode === self::SKIP_NOT_EXIST;
        if (!$versioning) {
            // ordinary update
            $res = $file->updateRms(!$skipNotExist, $upload);
            echo self::$debug ? "\t" . ($file->getCreated() ? "create " : "update ") . ($upload ? "+ upload " : "") . "\n" : '';
            return $res;
        } else {
            $oldRes = $file->createNewVersion($upload, $this->pidPass === self::PID_PASS);
            $newRes = $file->getResource(false);
            echo self::$debug ? "\tnewVersion" . ($upload ? " + upload " : "") . "\n" : '';
            echo self::$debug ? "\t" . $oldRes->getUri() . " ->\n" : '';

            return $newRes;
        }
    }

    private function readRepoConfig(): void {
        $c                     = $this->repo->getSchema()->ingest;
        $this->binaryClass     = $c->defaultBinaryClass;
        $this->collectionClass = $c->defaultCollectionClass;
        UriNormalizer::init((array) ($this->repo->getSchema()->uriNorm ?? []));
    }

    private function getFileHash(string $path, string $hashName): string {
        $hash = hash_init($hashName);
        hash_update_file($hash, $path);
        return hash_final($hash, false);
    }

}
