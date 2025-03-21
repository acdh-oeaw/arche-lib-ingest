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

use Throwable;
use SplObjectStorage;
use InvalidArgumentException;
use GuzzleHttp\Promise\RejectedPromise;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\ConnectException;
use rdfInterface\BlankNodeInterface;
use rdfInterface\TermInterface;
use rdfInterface\QuadInterface;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdf\DataFactory as DF;
use quickRdfIo\Util as RdfUtil;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use termTemplates\LiteralTemplate;
use termTemplates\NamedNodeTemplate;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\exception\Conflict;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\UriNormalizer;

/**
 * Class for importing whole metadata graph into the repository.
 *
 * @author zozlak
 */
class MetadataCollection extends Dataset {

    const SKIP                           = 1;
    const CREATE                         = 2;
    const ERRMODE_FAIL                   = 'fail';
    const ERRMODE_PASS                   = 'pass';
    const ERRMODE_INCLUDE                = 'include';
    const NETWORKERROR_SLEEP             = 3;
    const ALLOWED_CONFLICT_REASONS_REGEX = '/Resource [0-9]+ locked|Transaction [0-9]+ locked|Owned by other request|Lock not available|duplicate key value|deadlock detected/';

    /**
     * Turns debug messages on.
     * 
     * There are three levels:
     * 
     * - `false` or `0` - no debug messages at all
     * - `true` or `1` - basic information on preprocessing stages and detailed 
     *   information on ingestion progress
     * - `2` - detailed information on both preprocessing and ingestion
     *   progress
     */
    static public bool | int $debug = false;

    /**
     * Repository connection object
     */
    protected Repo $repo;

    /**
     * Parent resource for all imported graph nodes
     */
    private ?RepoResource $resource = null;

    /**
     * Should the title property be added automatically for ingested resources
     * missing it.
     */
    protected bool $addTitle = false;

    /**
     * Number of resource automatically triggering a commit (0 - no auto commit)
     */
    private int $autoCommit = 0;

    /**
     * Is the metadata graph preprocessed already?
     */
    private bool $preprocessed = false;
    private Schema $schema;
    private UriNormalizer $normalizer;

    /**
     * Creates a new metadata parser.
     * 
     * @param Repo $repo
     * @param \Psr\Http\Message\ResponseInterface | \Psr\Http\Message\StreamInterface | resource | string | null $input 
     *   Input to be parsed as RDF. Passed to the \quickRdfIo\Util::parse().
     * @param string|null $format
     * @see \quickRdfIo\Util::parse
     */
    public function __construct(Repo $repo, mixed $input, ?string $format = null) {
        parent::__construct();

        if ($input !== null) {
            $this->add(RdfUtil::parse($input, new DF(), $format));
        }

        $this->repo       = $repo;
        $this->schema     = $repo->getSchema();
        $this->normalizer = new UriNormalizer(null, $this->schema->id, null, null, new DF());
    }

    /**
     * Sets the repository resource being parent of all resources in the
     * graph imported by the import() method.
     * 
     * @param ?RepoResource $res
     * @return MetadataCollection
     * @see import()
     */
    public function setResource(?RepoResource $res): MetadataCollection {
        $this->resource = $res;
        return $this;
    }

    /**
     * Sets if the title property should be automatically added for ingested
     * resources which are missing it.
     * 
     * @param bool $add
     * @return MetadataCollection
     */
    public function setAddTitle(bool $add): MetadataCollection {
        $this->addTitle = $add;
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
     * @return MetadataCollection
     */
    public function setAutoCommit(int $count): MetadataCollection {
        $this->autoCommit = $count;
        return $this;
    }

    /**
     * Performs preprocessing - removes literal IDs, promotes URIs to IDs, etc.
     * 
     * @return MetadataCollection
     */
    public function preprocess(): MetadataCollection {
        $this->removeLiteralIds();
        $this->promoteUrisToIds();
        $this->promoteBNodesToUris();
        $this->fixReferences();
        $this->preprocessed = true;
        return $this;
    }

    /**
     * Imports the whole graph by looping over all resources.
     * 
     * A repository resource is created for every node containing at least one 
     * identifer and:
     * - with at least one outgoing edge (there's at least one triple having
     *   the node as a subject) of property other than identifier property
     * - or being within $namespace
     * - or when $singleOutNmsp equals to MetadataCollection::CREATE
     * 
     * Resources without identifier property are skipped as we are unable
     * to identify them on the next import (which would lead to duplication).
     * 
     * Resource with a fully qualified URI is considered as having
     * the identifier property value (its URI is promoted to it).
     * 
     * Resources in the graph can denote relationships in any way but all
     * object URIs already existing in the repository and all object URIs in the
     * $namespace will be turned into ACDH ids.
     * 
     * @param string $namespace repository resources will be created for all
     *   resources in this namespace
     * @param int $singleOutNmsp should repository resources be created
     *   representing URIs outside $namespace (MetadataCollection::SKIP or
     *   MetadataCollection::CREATE)
     * @param string $errorMode what should happen if an error is encountered?
     *   One of:
     *   - MetadataCollection::ERRMODE_FAIL - the first encountered error throws
     *     an exception.
     *   - MetadataCollection::ERRMODE_PASS - the first encountered error turns 
     *     off the autocommit but ingestion is continued. When all resources are 
     *     processed and there was no errors, an array of RepoResource objects 
     *     is returned. If there was an error, an exception is thrown.
     *   - MetadataCollection::ERRMODE_INCLUDE - the first encountered error 
     *     turns off the autocommit but ingestion is continued. The returned 
     *     array contains RepoResource objects for successful ingestions and
     *     Exception objects for failed ones.
     * @param int $concurrency number of parallel requests to the repository
     *   allowed during the import
     * @param int $retries how many ingestion attempts should be taken if the 
     *   repository resource is locked by other request or a network error occurs
     * @return array<RepoResource|ClientException>
     * @throws InvalidArgumentException
     * @throws IndexerException
     * @throws ClientException
     */
    public function import(string $namespace, int $singleOutNmsp,
                           string $errorMode = self::ERRMODE_FAIL,
                           int $concurrency = 3, int $retries = 6): array {
        $idProp = $this->schema->id;

        $dict = [self::SKIP, self::CREATE];
        if (!in_array($singleOutNmsp, $dict)) {
            throw new InvalidArgumentException('singleOutNmsp parameters must be one of MetadataCollection::SKIP, MetadataCollection::CREATE');
        }
        if (!in_array($errorMode, [self::ERRMODE_FAIL, self::ERRMODE_PASS, self::ERRMODE_INCLUDE])) {
            throw new InvalidArgumentException('errorMode parameters must be one of MetadataCollection::ERRMODE_FAIL and MetadataCollection::ERRMODE_PASS');
        }

        if (!$this->preprocessed) {
            $this->preprocess();
        }
        $toBeImported = $this->filterResources($namespace, $singleOutNmsp);

        // The only possible way of performing an atomic "check if resources exists and create it if not"
        // is to try to create it, therefore the algorithm goes as follows:
        // - [promise1] try to create the resource
        //   - if it succeeded, just return the repo resource
        //   - if it failed check the reason
        //     - if it's something other than primary key violation, return a RejectedPromise
        //       (which will be handled accordingly to the $mapErrorMode
        //     - it it's primary key violation such a resource exists already so:
        //       - [promise2] find it
        //         - [promise3] update its metadata
        $GN           = count($toBeImported);
        $Gn           = 0;
        $reingestions = [];
        $f            = function (TermInterface $sbj, Repo $repo) use (&$Gn,
                                                                       $GN,
                                                                       $idProp,
                                                                       &$reingestions) {
            $Gn++;
            $uri      = (string) $sbj;
            /** @phpstan-ignore isset.offset */
            $progress = "($Gn/$GN)" . (isset($reingestions[$uri]) ? " - $reingestions[$uri] reattempt" : '');
            echo self::$debug ? "Importing $uri $progress\n" : "";
            $meta     = $this->sanitizeResource($sbj);
            $promise1 = $this->repo->createResourceAsync($meta);
            $promise1 = $promise1->then(
                function (RepoResource $repoRes) use ($progress) {
                    echo self::$debug ? "\tcreated " . $repoRes->getUri() . " $progress\n" : "";
                    return $repoRes;
                }
            );
            $promise1 = $promise1->otherwise(
                function ($reason) use ($meta, $idProp, $progress) {
                    if (!($reason instanceof Conflict)) {
                        return new RejectedPromise($reason);
                    }
                    $ids      = $meta->listObjects(new PT($idProp))->getValues();
                    $promise2 = $this->repo->getResourceByIdsAsync($ids);
                    $promise2 = $promise2->then(
                        function (RepoResource $repoRes) use ($meta, $progress) {
                            echo self::$debug ? "\tupdating " . $repoRes->getUri() . " $progress\n" : "";
                            $repoRes->setMetadata($meta);
                            $promise3 = $repoRes->updateMetadataAsync();
                            return $promise3 === null ? $repoRes : $promise3->then(fn() => $repoRes);
                        }
                    );
                    return $promise2;
                }
            );
            return $promise1;
        };

        $allRepoRes      = [];
        $commitedRepoRes = [];
        $errors          = '';
        $chunkSize       = $this->autoCommit > 0 ? $this->autoCommit : min(count($toBeImported), 100 * $concurrency);
        for ($i = 0; $i < count($toBeImported); $i += $chunkSize) {
            if ($this->autoCommit > 0 && $i > 0 && count($toBeImported) > $this->autoCommit && empty($errors)) {
                echo self::$debug ? "Autocommit\n" : '';
                $commitedRepoRes = $allRepoRes;
                $this->repo->commit();
                $this->repo->begin();
            }
            $chunk        = array_slice($toBeImported, $i, $chunkSize);
            $chunkSize    = min($chunkSize, count($chunk)); // not to loose repeating reinjections
            $chunkRepoRes = $this->repo->map($chunk, $f, $concurrency, Repo::REJECT_INCLUDE);
            $sleep        = false;
            foreach ($chunkRepoRes as $n => $j) {
                // handle reingestion on "HTTP 409 Conflict"
                $conflict     = $j instanceof Conflict && preg_match(self::ALLOWED_CONFLICT_REASONS_REGEX, $j->getMessage());
                $notFound     = $j instanceof NotFound;
                $networkError = $j instanceof ConnectException;
                if ($conflict || $notFound || $networkError) {
                    $metaRes            = $chunk[$n];
                    $uri                = (string) $metaRes;
                    $reingestions[$uri] = ($reingestions[$uri] ?? 0) + 1;
                    if ($reingestions[(string) $metaRes] <= $retries) {
                        $toBeImported[] = $metaRes;
                        $sleep          = $sleep || $networkError;
                    }
                } else {
                    // non-retryable errors
                    if ($j instanceof Throwable && $errorMode === self::ERRMODE_FAIL) {
                        throw new IndexerException("Error during import", IndexerException::ERROR_DURING_IMPORT, $j, $commitedRepoRes);
                    } elseif ($j instanceof Throwable) {
                        $msg    = $j instanceof ClientException ? $j->getResponse()->getStatusCode() . ' ' . $j->getResponse()->getBody() : $j->getMessage();
                        $msg    = $chunk[$n] . ": " . $msg . "(" . get_class($j) . ")";
                        $errors .= "\t$msg\n";
                        echo self::$debug ? "\tERROR while processing $msg\n" : '';
                        // terminal errors: transaction doesn't exist, database max connections reached
                        if (preg_match("/Transaction [0-9]+ doesn't exist|SQLSTATE\[08006\]/", $msg)) {
                            break 2;
                        }
                    }
                    if ($j instanceof RepoResource || $errorMode === self::ERRMODE_INCLUDE) {
                        $allRepoRes[] = $j;
                    }
                }
            }
            if ($sleep) {
                sleep(self::NETWORKERROR_SLEEP);
            }
            if ($concurrency > 2) {
                // if another attempt is needed, gradually reduce the concurrency
                $concurrency = $concurrency >> 1;
            }
        }
        if (!empty($errors) && $errorMode === self::ERRMODE_PASS) {
            throw new IndexerException("There was at least one error during the import:\n.$errors", IndexerException::ERROR_DURING_IMPORT, null, $commitedRepoRes);
        }

        return $allRepoRes;
    }

    /**
     * Returns set of resources to be imported skipping all other.
     * @param string $namespace repository resources will be created for all
     *   resources in this namespace
     * @param int $singleOutNmsp should repository resources be created
     *   for URIs outside $namespace (MetadataCollection::SKIP or
     *   MetadataCollection::CREATE)
     * @return array<TermInterface>
     */
    private function filterResources(string $namespace, int $singleOutNmsp): array {
        $idProp = $this->schema->id;
        $nnTmpl = new NamedNodeTemplate(null, NamedNodeTemplate::ANY);
        $t0     = time();

        echo self::$debug ? "Filtering resources...\n" : '';

        if ($singleOutNmsp === MetadataCollection::CREATE) {
            $valid = iterator_to_array($this->listSubjects(new QT($nnTmpl, $idProp)));
        } else {
            // accept only subjects having (at least one) ID in $namespace 
            // or (at lest one) non-ID property
            $validTmp = new SplObjectStorage();
            foreach ($this->getIterator(new QT($nnTmpl)) as $quad) {
                $sbj       = $quad->getSubject();
                $nonIdCond = !$idProp->equals($quad->getPredicate());
                $nmspCond  = str_starts_with((string) $quad->getObject(), $namespace);
                if ($nonIdCond || $nmspCond) {
                    $validTmp->attach($sbj);
                }

                $t1 = time();
                if ($t1 > $t0 && $this->repo->inTransaction()) {
                    $this->repo->prolong();
                    $t0 = $t1;
                }
            }
            $valid = [];
            foreach ($validTmp as $sbj) {
                $valid[] = $sbj;
            }
        }
        if (self::$debug) {
            $skipSbj = $this->listSubjects()->getValues();
            $skipSbj = array_diff($skipSbj, array_map(fn($x) => (string) $x, $valid));
            foreach ($skipSbj as $i) {
                echo "\t$i skipping\n";
            }
        }
        if (self::$debug > 1) {
            foreach ($valid as $i) {
                echo "\t$i including\n";
            }
        }

        return $valid;
    }

    /**
     * To avoid creation of duplicated resources it must be assured every
     * resource is referenced acrossed the whole graph with only one URI
     * 
     * As it doesn't matter which exactly, the resource URI itself is
     * a convenient choice
     * 
     * @return void
     */
    private function fixReferences(): void {
        echo self::$debug ? "Fixing references...\n" : '';
        $idProp  = $this->schema->id;
        // must materialize because we are going to modify the dataset
        $idQuads = iterator_to_array($this->getIterator(new PT($idProp)));
        foreach ($idQuads as $quad) {
            // do not need to process "x id x"
            if (!$quad->getSubject()->equals($quad->getObject())) {
                foreach ($this->getIterator(new PT(null, $quad->getObject())) as $i) {
                    // do not need to process "any id x"
                    if (!$idProp->equals($i->getPredicate())) {
                        $this[$i] = $i->withObject($quad->getSubject());
                    }
                }
            }
        }
    }

    /**
     * Promotes BNodes to their first ID and fixes references to them.
     */
    private function promoteBNodesToUris(): void {
        echo self::$debug ? "Promoting BNodes to URIs...\n" : '';
        $idTmpl = new PT($this->schema->id);
        foreach ($this->listSubjects() as $sbj) {
            if ($sbj instanceof BlankNodeInterface) {
                $id = $this->getObject($idTmpl->withSubject($sbj));
                if ($id !== null) {
                    echo self::$debug ? "\t$sbj => $id\n" : '';
                    // fix subjects
                    $clbck = fn(QuadInterface $x) => $x->withSubject($id);
                    $this->forEach($clbck, new QT($sbj));
                    // fix objects
                    $clbck = fn(QuadInterface $x) => $x->withObject($id);
                    $this->forEach($clbck, new PT(null, $sbj));
                }
            }
        }
    }

    /**
     * Promotes subjects being fully qualified URLs to ids.
     */
    private function promoteUrisToIds(): void {
        $idProp = $this->schema->id;
        echo self::$debug ? "Promoting URIs to ids...\n" : '';
        foreach ($this->listSubjects() as $i) {
            if (!$i instanceof BlankNodeInterface) {
                $quad = DF::quad($i, $idProp, $i);
                if (!isset($this[$quad])) {
                    $this->add($quad);
                    echo self::$debug > 1 ? "t$i\n" : '';
                }
            }
        }
    }

    /**
     * Cleans up resource metadata.
     * 
     * @throws InvalidArgumentException
     */
    private function sanitizeResource(TermInterface $res): DatasetNode {
        $idProp    = $this->schema->id;
        $titleProp = $this->schema->label;
        $relProp   = $this->schema->parent;

        $meta = $this->copy(new QT($res));

        $nonIdProps = iterator_to_array($meta->listPredicates()->skip([$idProp]));
        // don't do anything when it's purely-id resource
        if (count($nonIdProps) == 0) {
            return (new DatasetNode($res))->withDataset($meta);
        }

        if ($meta->any(fn(QuadInterface $x) => $x->getObject() instanceof BlankNodeInterface)) {
            echo "$meta\n";
            throw new InvalidArgumentException('resource contains references to blank nodes');
        }

        $clbck = fn(QuadInterface $x) => $x->withObject(DF::namedNode($this->normalizer->normalize($x->getObject(), false)));
        $meta->forEach($clbck, new PT($idProp));

        if ($this->addTitle && $meta->none(new PT($titleProp))) {
            $id = $meta->getObject(new PT($idProp));
            $meta->add(DF::quad($res, $titleProp, DF::literal((string) $id, 'und')));
        }

        if ($this->resource !== null) {
            $meta->add(DF::quad($res, $relProp, $this->resource->getUri()));
        }

        return (new DatasetNode($res))->withDataset($meta);
    }

    /**
     * Removes literal ids from the graph.
     */
    private function removeLiteralIds(): void {
        echo self::$debug ? "Removing literal ids...\n" : "";
        $idProp = $this->schema->id;

        $removed = $this->delete(new PT($idProp, new LiteralTemplate(null, LiteralTemplate::ANY)));
        if (self::$debug) {
            foreach ($removed as $i) {
                echo "\tremoved $i\n";
            }
        }
    }
}
