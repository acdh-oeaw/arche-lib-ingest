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

namespace acdhOeaw\arche\lib\ingest;

use RuntimeException;
use BadMethodCallException;
use Throwable;
use DirectoryIterator;
use SplFileInfo;
use EasyRdf\Graph;
use zozlak\RdfConstants as RDF;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\ConnectException;
use acdhOeaw\UriNormalizer;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\exception\Conflict;
use acdhOeaw\arche\lib\ingest\util\FileId;
use acdhOeaw\arche\lib\ingest\util\ProgressMeter;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupConstant;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupInterface;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupException;

/**
 * Ingests files into the repository
 *
 * @author zozlak
 */
class Indexer {

    const FILTER_MATCH       = 1;
    const FILTER_SKIP        = 2;
    const SKIP_NONE          = 0;
    const SKIP_NOT_EXIST     = 1;
    const SKIP_EXIST         = 2;
    const SKIP_BINARY_EXIST  = 4;
    const VERSIONING_NONE    = 1;
    const VERSIONING_ALWAYS  = 2;
    const VERSIONING_DIGEST  = 3;
    const VERSIONING_DATE    = 4;
    const PID_KEEP           = 1;
    const PID_PASS           = 2;
    const ERRMODE_FAIL       = 'fail';
    const ERRMODE_PASS       = 'pass';
    const ERRMODE_INCLUDE    = 'include';
    const ERRMODE_CONTINUE   = 'continue';
    const NETWORKERROR_SLEEP = 3;

    /**
     * Turns debug messages on
     */
    static public bool $debug = false;
    private FileId $idgen;

    /**
     * RepoResource which children are created by the Indexer
     */
    private RepoResource $parent;

    /**
     * Regular expression for matching child resource file names.
     */
    private string $filter = '';

    /**
     * Regular expression for excluding child resource file names.
     */
    private string $filterNot = '';

    /**
     * Should children be directly attached to the RepoResource or maybe
     * each subdirectory should result in a separate collection resource
     * containing its children.
     */
    private bool $flatStructure = false;

    /**
     * Maximum size of a child resource (in bytes) resulting in the creation
     * of binary resources.
     * 
     * For child resources bigger then this limit an "RDF only" repository 
     * resources will be created.
     * 
     * Special value of -1 means "import all no matter their size"
     */
    private int $uploadSizeLimit = -1;

    /**
     * URI of an RDF class assigned to indexed collections.
     */
    private ?string $collectionClass = null;

    /**
     * URI of an RDF class assigned to indexed binary resources.
     */
    private ?string $binaryClass = null;

    /**
     * How many subsequent subdirectories should be indexed.
     */
    private int $depth = \PHP_INT_MAX;

    /**
     * Number of resource automatically triggering a commit (0 - no auto commit)
     */
    private int $autoCommit = 0;

    /**
     * Base ingestion path to be substituted with the $idPrefix
     * to form a binary id.
     */
    private string $directory;

    /**
     * Namespaces to substitute the $directory in the ingested binary path
     * to form a binary id.
     */
    private ?string $idPrefix = null;

    /**
     * Should resources be created for empty directories.
     * 
     * Skipped if `$flatStructure` equals to `true`
     */
    private bool $includeEmpty = false;

    /**
     * Should files (not)existing in the repository be skipped?
     * @see setSkip()
     */
    private int $skipMode = self::SKIP_NONE;

    /**
     * Should new versions of binary resources already existing in the repository
     * be created (if not, an existing resource is simply overwritten).
     */
    private int $versioningMode = self::VERSIONING_NONE;

    /**
     * Should PIDs (epic handles) be migrated to the new version of a resource
     * during versioning.
     */
    private int $pidPass = self::PID_KEEP;

    /**
     * An object providing metadata when given a resource file path
     */
    private MetaLookupInterface $metaLookup;

    /**
     * 
     * @var UriNormalizer
     */
    private UriNormalizer $uriNorm;

    /**
     * Should files without external metadata (provided by the `$metaLookup`
     * object) be skipped.
     */
    private bool $metaLookupRequire = false;

    /**
     * Repository connection
     */
    private Repo $repo;

    /**
     * Repository schema
     */
    private Schema $schema;

    /**
     * Creates the Indexer object.
     * 
     * It's important to understand how the file/directory paths are mapped to
     * repository resource identifiers:
     * 
     * - first the path is stripped from the `$directory`
     * - the rest is URL-encoded but with '/' characters being preserved
     * - at the end the $idPrefix is prepended
     * 
     * e.g. for a file `/foo/foo bar/baz` with `$directory` `/foo/` and 
     * `$idPrefix` `https://id.nmsp/foobar/`, it will be done as follows:
     * 
     * - `foo/foo bar/baz` => `foo bar/baz`
     * - `foo bar/baz` => `foo%20bar/baz`
     * - `foo%20bar/baz` => `https://id.nmsp/foobar/foo%20bar/baz`
     * 
     * It's worth mentioning (lack of) slashes at the end of `$directory` and
     * `$idPrefix` doesn't matter (it's standardized internally).
     * 
     * @param string $directory path to be indexed.
     * @param string $idPrefix prefix used to create repository
     *   resource identifiers from file/directory paths (see above).
     * @param Repo $repo repository connectiond object.
     */
    public function __construct(string $directory, string $idPrefix, Repo $repo) {
        $this->directory = $directory;
        $this->idPrefix  = $idPrefix;
        $this->repo      = $repo;

        if (substr($this->directory, -1) === '/') {
            $this->directory = substr($this->directory, 0, -1);
        }

        $this->schema          = $this->repo->getSchema();
        $this->binaryClass     = $this->schema->ingest->defaultBinaryClass;
        $this->collectionClass = $this->schema->ingest->defaultCollectionClass;
        $this->uriNorm         = new UriNormalizer(null, $this->schema->id);
        $this->metaLookup      = new MetaLookupConstant((new Graph())->resource('.'));
    }

    /**
     * Sets the parent resource for the files in the `$directory` (constructor 
     * parameter) directory.
     * 
     * @param RepoResource $resource
     */
    public function setParent(RepoResource $resource): Indexer {
        $this->parent = $resource;
        return $this;
    }

    /**
     * Controls the automatic commit behaviour.
     * 
     * Even when you use autocommit, you should commit your transaction after
     * `Indexer::index()` (the only exception is when you set auto commit to 1
     * forcing commiting each and every resource separately but you probably 
     * don't want to do that for performance reasons).
     * 
     * @param int $count number of resource automatically triggering a commit 
     *   (0 - no auto commit)
     * @return Indexer
     */
    public function setAutoCommit(int $count): Indexer {
        $this->autoCommit = $count;
        return $this;
    }

    /**
     * Defines if (and how) resources should be skipped from indexing based on
     * their (not)existance in the repository.
     * 
     * @param int $skipMode mode. Any combination of (with a binary union or sum):
     *   - Indexer::SKIP_NONE (default) - import all resources, 
     *   - Indexer::SKIP_NOT_EXIST - skip ingestion of all resources which don't
     *     exist at the beginning of the ingestion.
     *   - Indexer::SKIP_EXIST - skip ingestion of all resources which exist
     *     at the beginning of the ingestion.
     *   - Indexer::SKIP_BINARY_EXIST - skip ingestion of all resources which
     *     already exist and either already have binary payload or are metadata
     *     only resources.
     * @return Indexer
     */
    public function setSkip(int $skipMode): Indexer {
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
     * @return Indexer
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
     * @return Indexer
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
     * @return Indexer
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
     * @return Indexer
     */
    public function setFilter(string $filter, int $type = self::FILTER_MATCH): Indexer {
        switch ($type) {
            case self::FILTER_MATCH:
                $this->filter    = $filter;
                break;
            case self::FILTER_SKIP:
                $this->filterNot = $filter;
                break;
            default:
                throw new BadMethodCallException('wrong $type parameter');
        }
        return $this;
    }

    /**
     * Sets if child resources be directly attached to the indexed RepoResource
     * (`$isFlat` equals to `true`) or a separate collection repository resource
     * be created for each subdirectory (`$isFlat` equals to `false`).
     * 
     * @param bool $isFlat
     * @return Indexer
     */
    public function setFlatStructure(bool $isFlat): Indexer {
        $this->flatStructure = $isFlat;
        return $this;
    }

    /**
     * Sets size treshold for uploading child resources as binary resources.
     * 
     * For files bigger then this treshold a "pure RDF" repository resources will
     * be created containing full metadata but no binary content.
     * 
     * @param int $limit maximum size in bytes; 0 will cause no files upload,
     *   special value of -1 (default) will cause all files to be uploaded no 
     *   matter their size
     * @return Indexer
     */
    public function setUploadSizeLimit(int $limit): Indexer {
        $this->uploadSizeLimit = $limit;
        return $this;
    }

    /**
     * Sets maximum indexing depth.
     * 
     * @param int $depth maximum indexing depth (0 - only initial resource dir, 1 - also its direct subdirectories, etc.)
     * @return Indexer
     */
    public function setDepth(int $depth): Indexer {
        $this->depth = $depth;
        return $this;
    }

    /**
     * Sets if repository resources should be created for empty directories.
     * 
     * @param bool $include should resources be created for empty directories
     * @return Indexer
     * @see setFlatStructure()
     */
    public function setIncludeEmptyDirs(bool $include): Indexer {
        $this->includeEmpty = $include;
        return $this;
    }

    /**
     * Sets a class providing metadata for indexed files.
     * @param MetaLookupInterface $metaLookup
     * @param bool $require should files lacking external metadata be skipped
     * @return Indexer
     */
    public function setMetaLookup(MetaLookupInterface $metaLookup,
                                  bool $require = false): Indexer {
        $this->metaLookup        = $metaLookup;
        $this->metaLookupRequire = $require;
        return $this;
    }

    /**
     * Performs the indexing.
     * 
     * @param string $errorMode what should happen if an error is encountered?
     *   One of:
     *   - Indexer::ERRMODE_FAIL - the first encountered error throws
     *     an exception.
     *   - Indexer::ERRMODE_PASS - the first encountered error turns 
     *     off the autocommit but ingestion is continued. When all resources are 
     *     processed andif there were no errors, an array of RepoResource objects 
     *     is returned. If there was an error, an exception is thrown.
     *   - Indexer::ERRMODE_INCLUDE - the first encountered error 
     *     turns off the autocommit but ingestion is continued. The returned 
     *     array contains RepoResource objects for successful ingestions and
     *     Exception objects for failed ones.
     *   - Indexer::ERRMODE_CONTINUE - just continue the import no matter if
     *     errors were encountered. The returned array contains RepoResource 
     *     objects for successful ingestions and Exception objects for failed ones.
     * @param int $concurrency number of parallel requests to the repository
     *   allowed during the import
     * @param int $retries how many ingestion attempts should be taken if the
     *   repository resource is locked by other request or an network connection 
     *   error occurs
     * @return array<RepoResource|ClientException> a list RepoResource objects representing indexed resources
     * @throws IndexerException
     */
    public function import(string $errorMode = self::ERRMODE_FAIL,
                           int $concurrency = 3, int $retries = 6): array {
        if (!isset($this->repo)) {
            throw new IndexerException("Repository connection object isn't set. Call setRepo() or setParent() first or pass the Repo object to the constructor.");
        }
        $pidPass = $this->pidPass === self::PID_PASS;

        if ($this->flatStructure && substr($this->idPrefix, -1) !== '/') {
            $this->idPrefix .= '/';
        }
        $this->idgen = new FileId($this->idPrefix, $this->flatStructure ? '' : $this->directory);

        // gather files from the filesystem
        $filesToImport = $this->listFiles(new SplFileInfo($this->directory), 0);
        $meterId       = self::$debug ? (string) microtime(true) : null;
        if ($meterId !== null) {
            ProgressMeter::init($meterId, count($filesToImport));
        }

        // ingest
        $f               = fn(File $file) => $file->uploadAsync($this->uploadSizeLimit, $this->skipMode, $this->versioningMode, $pidPass, $meterId);
        $allRepoRes      = [];
        $commitedRepoRes = [];
        $errors          = '';
        $chunkSize       = $this->autoCommit > 0 ? $this->autoCommit : min(count($filesToImport), 100 * $concurrency);
        for ($i = 0; $i < count($filesToImport); $i += $chunkSize) {
            if ($this->autoCommit > 0 && $i > 0 && count($filesToImport) > $this->autoCommit && (empty($errors) || $errorMode === self::ERRMODE_CONTINUE)) {
                echo self::$debug ? "Autocommit\n" : '';
                $commitedRepoRes = $allRepoRes;
                $this->repo->commit();
                $this->repo->begin();
            }
            $chunk        = array_slice($filesToImport, $i, $chunkSize);
            $chunkSize    = min($chunkSize, count($chunk)); // not to loose repeating reinjections
            $chunkRepoRes = $this->repo->map($chunk, $f, $concurrency, Repo::REJECT_INCLUDE);
            $sleep        = false;
            foreach ($chunkRepoRes as $n => $j) {
                if ($j instanceof SkippedException) {
                    continue;
                }
                // handle reingestion on "HTTP 409 Conflict"
                $isConflict     = $j instanceof Conflict && preg_match('/Resource [0-9]+ locked|Transaction [0-9]+ locked|Owned by other request|Lock not available|duplicate key value/', $j->getMessage());
                $isNetworkError = $j instanceof ConnectException;
                if (($isConflict || $isNetworkError) && $chunk[$n]->getUploadsCount() <= $retries + 1) {
                    $filesToImport[] = $chunk[$n];
                    $sleep           = $sleep || $isNetworkError;
                } else {
                    // non-retryable errors
                    if ($j instanceof Throwable && $errorMode === self::ERRMODE_FAIL) {
                        throw new IndexerException("Error during import", IndexerException::ERROR_DURING_IMPORT, $j, $commitedRepoRes);
                    } elseif ($j instanceof Throwable) {
                        $msg    = $j instanceof ClientException ? $j->getResponse()->getBody() : $j->getMessage();
                        $msg    = $chunk[$n]->getPath() . ": " . $msg;
                        $errors .= "\t$msg\n";
                        echo self::$debug ? "\tERROR while processing " . $chunk[$n]->getPath() . ": $msg\n" : '';
                    }
                    if ($j instanceof RepoResource || $errorMode === self::ERRMODE_INCLUDE || $errorMode === self::ERRMODE_CONTINUE) {
                        $allRepoRes[] = $j;
                    }
                }
            }
            if ($sleep) {
                sleep(self::NETWORKERROR_SLEEP);
            }
        }
        if (!empty($errors) && $errorMode === self::ERRMODE_PASS) {
            throw new IndexerException("There was at least one error during the import:\n.$errors", IndexerException::ERROR_DURING_IMPORT, null, $commitedRepoRes);
        }
        return $allRepoRes;
    }

    /**
     * Gets the list of files/dirs matching the filename filters and the depth
     * limit.
     * 
     * @return array<File>
     */
    private function listFiles(SplFileInfo $dir, int $level): array {
        $t0    = time();
        $iter  = new DirectoryIterator($dir->getPathname());
        $files = [];
        foreach ($iter as $n => $file) {
            if ($file->isFile()) {
                $filterMatch = empty($this->filter) || preg_match($this->filter, $file->getFilename());
                $filterSkip  = empty($this->filterNot) || !preg_match($this->filterNot, $file->getFilename());
                if ($filterMatch && $filterSkip) {
                    try {
                        $files[] = $this->createFile($file->getFileInfo());
                    } catch (MetaLookupException) {
                        
                    }
                }
            } elseif ($file->isDir() && !$file->isDot() && $level < $this->depth) {
                $files = array_merge($files, $this->listFiles($file, $level + 1));
            }
            $t = time();
            if ($t - $t0 > 10 && $this->repo->inTransaction()) {
                $this->repo->prolong();
                $t0 = $t;
            }
        }
        if (!$this->flatStructure && $level > 0 && (($n ?? 0) > 0 || $this->includeEmpty)) {
            try {
                $files[] = $this->createFile($dir->getFileInfo());
            } catch (MetaLookupException) {
                
            }
        }
        return $files;
    }

    private function createFile(SplFileInfo $file): File {
        $path   = $file->getPathname();
        $dir    = $file->getPath();
        $schema = $this->repo->getSchema();

        $id = match ($this->flatStructure) {
            true => $this->idgen->getId($file->getFilename(), ''),
            false => $this->idgen->getId($path)
        };

        $extMeta = $this->metaLookup->getMetadata($path, [$id], $this->metaLookupRequire);
        // id
        $extMeta->addResource($schema->id, $id);
        // filename
        $extMeta->addLiteral($schema->fileName, $file->getFilename());
        // class
        if ($file->isDir() && !empty($this->collectionClass)) {
            $extMeta->addResource(RDF::RDF_TYPE, $this->collectionClass);
        } else if ($file->isFile() && !empty($this->binaryClass)) {
            $extMeta->addResource(RDF::RDF_TYPE, $this->binaryClass);
        }
        // parent
        if (isset($this->parent) && ($this->flatStructure || $dir === $this->directory)) {
            $extMeta->addResource($schema->parent, $this->parent->getUri());
        }
        if ($dir !== $this->directory && !$this->flatStructure) {
            $extMeta->addResource($schema->parent, substr($id, 0, strrpos($id, '/') ?: null));
        }
        // mime type and binary size
        if ($file->isFile()) {
            $extMeta->addLiteral($schema->binarySize, $file->getSize());
            $mime = BinaryPayload::guzzleMimetype($path);
            $mime ??= mime_content_type($path);
            if (!empty($mime)) {
                $extMeta->addLiteral($schema->mime, $mime);
            }
        }
        // normalize ids
        $this->uriNorm->normalizeMeta($extMeta, '', false);

        return new File($file, $extMeta, $this->repo);
    }
}
