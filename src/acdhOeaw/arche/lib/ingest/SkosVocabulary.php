<?php

/*
 * The MIT License
 *
 * Copyright 2022 zozlak.
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

use BadMethodCallException;
use RuntimeException;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Promise\RejectedPromise;
use zozlak\RdfConstants as RDF;
use rdfInterface\LiteralInterface;
use rdfInterface\NamedNodeInterface;
use rdfInterface\QuadInterface;
use rdfInterface\TermInterface;
use quickRdf\DataFactory as DF;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use termTemplates\NamedNodeTemplate;
use termTemplates\AnyOfTemplate;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\SearchConfig;
use acdhOeaw\arche\lib\SearchTerm;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\RepoResourceInterface as RRI;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\RepoLibException;

;

/**
 * A specialization of the MetadataCollection class for ingesting SKOS
 * vocabularies.
 *
 * Given an RDF graph with exactly one node of type skos:ConceptSchema it
 * ingests the skos:ConceptSchema and skos:Concept nodes as well as, 
 * depending on the configuration:
 * 
 * - skos:Collection and skos:OrderedCollection nodes
 * - nodes being RDF triple objects in above-mentioned nodes
 * 
 * All other nodes in the RDF graph are removed by the `preprocess()` method.
 * 
 * @author zozlak
 */
class SkosVocabulary extends MetadataCollection {

    const NMSP_SKOS          = 'http://www.w3.org/2004/02/skos/core#';
    const NMSP_DC            = 'http://purl.org/dc/elements/1.1/';
    const NMSP_DCT           = 'http://purl.org/dc/terms/';
    const EXACTMATCH_KEEP    = 'keep';
    const EXACTMATCH_DROP    = 'drop';
    const EXACTMATCH_MERGE   = 'merge';
    const EXACTMATCH_LITERAL = 'literal';
    const STATE_NEW          = 'new';
    const STATE_OK           = 'ok';
    const STATE_UPDATE       = 'update';
    const RELATIONS_KEEP     = 'keep';
    const RELATIONS_DROP     = 'drop';
    const RELATIONS_LITERAL  = 'literal';

    /**
     * 
     * @var array<string>
     */
    static private array $skosRelations = [
        RDF::SKOS_BROADER,
        RDF::SKOS_BROADER_TRANSITIVE,
        RDF::SKOS_BROAD_MATCH,
        RDF::SKOS_CLOSE_MATCH,
        RDF::SKOS_EXACT_MATCH,
        RDF::SKOS_HAS_TOP_CONCEPT,
        RDF::SKOS_IN_SCHEME,
        RDF::SKOS_MAPPING_RELATION,
        RDF::SKOS_NARROWER,
        RDF::SKOS_NARROWER_TRANSITIVE,
        RDF::SKOS_NARROW_MATCH,
        RDF::SKOS_RELATED,
        RDF::SKOS_RELATED_MATCH,
        RDF::SKOS_SEMANTIC_RELATION,
        RDF::SKOS_TOP_CONCEPT_OF,
    ];

    static public function fromUrl(Repo $repo, string $url): self {
        $tmpFile = (string) tempnam(sys_get_temp_dir(), 'skosvocab');
        $client  = new Client();
        $headers = ['Accept' => 'text/turtle;q=1, application/rdf+xml;q=0.8, application/n-triples;q=0.6, application/ld+json;q=0.4'];
        $resp    = $client->send(new Request('get', $url, $headers));
        $format  = $resp->getHeader('Content-Type')[0] ?? '';
        $format  = explode(';', $format)[0];
        file_put_contents($tmpFile, (string) $resp->getBody());
        try {
            return new self($repo, $tmpFile, $format, $url);
        } finally {
            unlink($tmpFile);
        }
    }

    private string $file;
    private string $format;
    private NamedNodeInterface $vocabularyUrl;
    private string $state;

    /**
     * 
     * @var array<string>|null
     */
    private ?array $allowedNmsp = null;

    /**
     * 
     * @var array<string>|null
     */
    private ?array $allowedResourceNmsp = [];

    /**
     * How to handle skos:exactMatch triples with object outside the current vocabulary
     */
    private string $exactMatchMode = self::EXACTMATCH_MERGE;

    /**
     * How to handle skos:exactMatch triples with object within the current vocabulary
     */
    private string $exactMatchModeSchema = self::EXACTMATCH_MERGE;

    /**
     * How to handle skos:semanticRelation triples other then skos:exactMatch
     * with object outside the current vocabulary
     */
    private string $relationsMode = self::RELATIONS_DROP;

    /**
     * How to handle skos:semanticRelation triples other then skos:exactMatch
     * with object within the current vocabulary
     */
    private string $relationsModeSchema = self::RELATIONS_KEEP;

    /**
     * Should skos:Collection and skos:OrderedCollection resources be ingested?
     */
    private bool $importCollections = false;

    /**
     * Should skos:concept, skos:collection adn skos:orderedCollection resources
     * be connected with the skos:schema repository resource with the repository's
     * parent RDF property?
     * @var bool
     */
    private bool $addParentProperty = true;

    /**
     * RDF properties to use for repository resource titles.
     * @var array<string>
     */
    private array $titleProperties = [RDF::SKOS_PREF_LABEL, RDF::SKOS_ALT_LABEL];

    public function __construct(Repo $repo, string $file, mixed $format = null,
                                ?string $uri = null) {
        parent::__construct($repo, $file, $format);
        $schema = $this->repo->getSchema();

        if (!empty($this->format)) {
            $this->format = (string) $format;
        }

        // make a copy of the vocabulary file as it will be needed on import()
        $this->file = (string) tempnam(sys_get_temp_dir(), 'skosvocab');
        copy($file, $this->file);

        // make sure the RDF graph contains exactly one vocabulary
        $schemas = $this->listSubjects(new PT(DF::namedNode(RDF::RDF_TYPE), DF::namedNode(RDF::SKOS_CONCEPT_SCHEMA)));
        $schemas = iterator_to_array($schemas);
        if (count($schemas) === 0) {
            throw new RuntimeException("No skos:ConceptSchema found in the RDF graph");
        }
        if (count($schemas) > 1) {
            throw new RuntimeException("Many skos:ConceptSchema found in the RDF graph");
        }
        $url = current($schemas);
        if (!($url instanceof NamedNodeInterface)) {
            throw new RuntimeException("Schema URL is not a named node");
        }
        $this->vocabularyUrl = $url;
        if (!empty($uri)) {
            $this->add(DF::quad($this->vocabularyUrl, $schema->id, DF::namedNode($uri)));
        }

        // check if the vocabulary is up to date
        $repoRes     = $hash        = $repoResHash = null;
        try {
            $repoRes     = $this->repo->getResourceById($this->vocabularyUrl);
            $repoRes->loadMetadata(false, RRI::META_RESOURCE, null, [$schema->hash]);
            list($hashName, $repoResHash) = explode(':', (string) ($repoRes->getGraph()->getObject(new PT($schema->hash)) ?? 'sha1:'));
            $hash        = hash_init($hashName);
            hash_update_file($hash, $file);
            $hash        = hash_final($hash, false);
            $this->state = $hash === $repoResHash ? self::STATE_OK : self::STATE_UPDATE;
        } catch (NotFound $ex) {
            $this->state = self::STATE_NEW;
            $hash        = '';
        }
        echo self::$debug ? "Vocabulary state $this->state ($this->vocabularyUrl, " . $repoRes?->getUri() . ", $hash, $repoResHash)\n" : '';
    }

    public function __destruct() {
        unlink($this->file);
    }

    /**
     * Returns the state of the vocabulary in the repository:
     * 
     * - SkosVocabulary::STATE_NEW - there's no such vocabulary in the repository
     * - SkosVocabulary::STATE_OK - the vocabulary is the same as in the repository
     * - SkosVocabulary::STATE_UPDATE - there is a corresponding vocabulary in
     *   the repository but it requires updating
     * @return string
     */
    public function getState(): string {
        return $this->state;
    }

    public function forceUpdate(): self {
        if ($this->state === self::STATE_OK) {
            $this->state = self::STATE_UPDATE;
        }
        return $this;
    }

    /**
     * Set RDF property filter for skos resources.
     * 
     * Repository id and label properties are always allowed.
     * 
     * @param array<string>|null $nmsp null allows all properties
     * @return self
     */
    public function setAllowedNamespaces(?array $nmsp): self {
        $this->allowedNmsp = $nmsp;
        return $this;
    }

    /**
     * Defines namespaces of RDF properties allowed to keep object values.
     * 
     * SKOS properties, id, parent and rdf:type RDF properties are always allowed
     * to have object values.
     * 
     * Object values of other properties of SKOS entities will be turned into
     * literals of type xsd:anyURI.
     * 
     * Such an approach prevents creation of unnecessary repository resources but
     * can lead to resulting data being incompatible with ontologies they were
     * following (as datatype and object properties are mutually exclusive in 
     * owl) which may or might be a problem for you.
     * 
     * @param array<string>|null $allowed List of allowed namespaces. When null,
     *   all object values are kept.
     * @return self
     */
    public function setAllowedResourceNamespaces(?array $allowed): self {
        $this->allowedResourceNmsp = $allowed;
        return $this;
    }

    /**
     * Sets up skos:exactMatch RDF triples handling where the object belongs or
     * not belongs to a current vocabulary.
     * 
     * Both parameters can take following values:
     * 
     * - `SkosVocabulary::EXACTMATCH_KEEP` - leave the triple as it is
     * - `SkosVocabulary::EXACTMATCH_DROP` - remove the triple
     * - `SkosVocabulary::EXACTMATCH_MERGE` - merge subject and object into one
     *   repository resource
     * - `SkosVocabulary::EXACTMATCH_LITERAL` - turn triple's object into
     *   a literal of type xsd:anyURI (please note it produces RDF which doesn't
     *   follow SKOS as SKOS relations are OWL object properties)
     *  
     * @param string $inVocabulary
     * @param string $notInVocabulary
     * @return self
     */
    public function setExactMatchMode(string $inVocabulary,
                                      string $notInVocabulary): self {
        $allowed = [
            self::EXACTMATCH_DROP, self::EXACTMATCH_KEEP,
            self::EXACTMATCH_MERGE, self::EXACTMATCH_LITERAL
        ];
        if (!in_array($inVocabulary, $allowed) || !in_array($notInVocabulary, $allowed)) {
            throw new BadMethodCallException("Wrong inVocabulary or notInVocabulary parameter value");
        }
        $this->exactMatchModeSchema = $inVocabulary;
        $this->exactMatchMode       = $notInVocabulary;
        return $this;
    }

    /**
     * Sets up skos:semanticRelation RDF triples handling where the object 
     * belongs or not belongs to a current vocabulary.
     * 
     * Both parameters can take following values:
     * 
     * - `SkosVocabulary::RELATION_KEEP` - leave the triple as it is
     * - `SkosVocabulary::RELATION_DROP` - remove the triple
     * - `SkosVocabulary::RELATION_LITERAL` - turn triple's object into
     *   a literal of type xsd:anyURI (please note it produces RDF which doesn't
     *   follow SKOS as SKOS relations are OWL object properties)
     * 
     * @param string $inVocabulary
     * @param string $notInVocabulary
     * @return self
     */
    public function setSkosRelationsMode(string $inVocabulary,
                                         string $notInVocabulary): self {
        $allowed = [self::RELATIONS_DROP, self::RELATIONS_KEEP, self::RELATIONS_LITERAL];
        if (!in_array($inVocabulary, $allowed) || !in_array($notInVocabulary, $allowed)) {
            throw new BadMethodCallException("Wrong inVocabulary or notInVocabulary parameter value");
        }
        $this->relationsModeSchema = $inVocabulary;
        $this->relationsMode       = $notInVocabulary;
        return $this;
    }

    /**
     * Sets up if skos:Collection and skos:OrderedCollection nodes should be
     * ingested into the repository.
     * 
     * @param bool $import
     * @return self
     */
    public function setImportCollections(bool $import): self {
        $this->importCollections = $import;
        return $this;
    }

    /**
     * When $add is set to true, all repository resources representing imported 
     * skos entities are linked with the skos:Schema repository resource with
     * a repository's parent property.
     * 
     * @param bool $add
     * @return self
     */
    public function setAddParentProperty(bool $add): self {
        $this->addParentProperty = $add;
        return $this;
    }

    /**
     * Sets up which RDF properties a repository resource title for skos
     * entities should be derived from.
     * 
     * First property providing a title value is being used.
     * 
     * @param array<string> $properties
     * @return self
     */
    public function setTitleProperties(array $properties): self {
        $this->titleProperties = $properties;
        return $this;
    }

    public function preprocess(): self {
        if ($this->state === self::STATE_OK) {
            echo self::$debug ? "Skipping preprocessing - vocabulary is up to date\n" : '';
            return $this;
        }
        $rdfTypeTmpl = new PT(DF::namedNode(RDF::RDF_TYPE));

        // find all concepts in the SKOS scheme
        $concepts    = $this->listSubjects(new PT(DF::namedNode(RDF::SKOS_IN_SCHEME), $this->vocabularyUrl));
        $concepts    = iterator_to_array($concepts);
        $collections = [];
        if ($this->importCollections) {
            $collections = [
                $this->listSubjects($rdfTypeTmpl->withObject(DF::namedNode(RDF::SKOS_COLLECTION))),
                $this->listSubjects($rdfTypeTmpl->withObject(DF::namedNode(RDF::SKOS_ORDERED_COLLECTION))),
            ];
            $collections = array_map(fn($x) => iterator_to_array($x), $collections);
            $collections = array_merge(...$collections);
        }

        // preprocess
        $concepts            = $this->processExactMatches($concepts);
        $entities            = array_merge([$this->vocabularyUrl], $concepts);
        $entitiesCollections = array_merge($entities, $collections);

        $this->processRelations($entities);
        $this->assureTitles($entitiesCollections);
        $this->dropProperties($entitiesCollections);
        $this->assureLiterals($entitiesCollections);
        $this->assureParents(array_diff($entitiesCollections, [$this->vocabularyUrl]));
        $this->dropNodes($entitiesCollections);
        parent::preprocess();

        return $this;
    }

    /**
     * Ingests the vocabulary and removes obsolete vocabulary entities (repository
     * resources which were not a part of the ingestion but point to the schema
     * repository resource with skos:inScheme or repoCfg:parent)
     * 
     * @param string $namespace
     * @param int $singleOutNmsp
     * @param string $errorMode
     * @param int $concurrency
     * @param int $retriesOnConflict
     * @return array<RepoResource|ClientException>
     */
    public function import(string $namespace = '',
                           int $singleOutNmsp = self::CREATE,
                           string $errorMode = self::ERRMODE_FAIL,
                           int $concurrency = 3, int $retriesOnConflict = 3): array {
        if ($this->state === self::STATE_OK) {
            echo self::$debug ? "Skipping import - vocabulary is up to date\n" : '';
            return [];
        }

        // perform the ingestion
        $imported = parent::import($namespace, $singleOutNmsp, $errorMode, $concurrency, $retriesOnConflict);

        // upload vocabulary binary
        echo self::$debug ? "Uploading vocabulary binary\n" : '';
        /** @var RepoResource $repoRes */
        $repoRes = $this->repo->getResourceById($this->vocabularyUrl);
        $payload = new BinaryPayload(null, $this->file, $this->format ?? null);
        echo self::$debug ? "Updating " . $repoRes->getUri() . "\n" : '';
        $repoRes->updateContent($payload, RRI::META_NONE);

        // remove obsolete entities
        $this->removeObsolete($imported, $concurrency, $retriesOnConflict);

        return $imported;
    }

    /**
     * 
     * @param array<NamedNodeInterface> $entities
     * @return void
     */
    private function processRelations(array $entities): void {
        if ($this->relationsMode === self::RELATIONS_KEEP && $this->relationsModeSchema === self::RELATIONS_KEEP) {
            return;
        }
        echo self::$debug ? "Processing skos relations...\n" : "";
        $xsdUri      = DF::namedNode(RDF::XSD_ANY_URI);
        $entitiesStr = array_map(fn($x) => (string) $x, $entities);
        $sbjTmpl     = new AnyOfTemplate($entities);
        $propTmpl    = new AnyOfTemplate(array_map(fn($x) => DF::namedNode($x), self::$skosRelations));
        foreach ($this->copy(new QT($sbjTmpl, $propTmpl)) as $relation) {
            $sbj  = $relation->getSubject();
            $prop = $relation->getPredicate();
            $obj  = $relation->getObject();
            $mode = in_array((string) $obj, $entitiesStr) ? $this->relationsModeSchema : $this->relationsMode;
            if ($mode === self::RELATIONS_DROP) {
                echo self::$debug > 1 ? "\tRemoving <$sbj> <$prop> <$obj>\n" : '';
                $this->delete($relation);
            } elseif ($mode === self::RELATIONS_LITERAL) {
                echo self::$debug > 1 ? "\tTurning <$sbj> <$prop> <$obj> into literal\n" : '';
                $this->delete($relation);
                $this->add(DF::quad($sbj, $prop, DF::literal((string) $obj, null, $xsdUri)));
            }
        }
    }

    /**
     * 
     * @param array<TermInterface> $entities
     * @return array<string>
     */
    private function processExactMatches(array $entities): array {
        if ($this->exactMatchMode === self::EXACTMATCH_KEEP && $this->exactMatchModeSchema === self::EXACTMATCH_KEEP) {
            return $entities;
        }
        $matchTmpl = new PT(DF::namedNode(RDF::SKOS_EXACT_MATCH));
        $xsdUri    = DF::namedNode(RDF::XSD_ANY_URI);
        $asLiteral = fn(QuadInterface $x) => $x->withObject(DF::literal((string) $x->getObject(), null, $xsdUri));

        echo self::$debug ? "Processing skos:exactMatch...\n" : "";
        $drop        = [];
        $entitiesStr = array_map(fn($x) => (string) $x, $entities);
        foreach ($entities as $sbj) {
            $tmpl = $matchTmpl->withSubject($sbj);
            foreach ($this->listObjects($tmpl) as $obj) {
                $mode = in_array((string) $obj, $entitiesStr) ? $this->exactMatchModeSchema : $this->exactMatchMode;
                if ($mode === self::EXACTMATCH_DROP) {
                    echo self::$debug > 1 ? "\tRemoving <$sbj> skos:exactMatch <$obj>\n" : '';
                    $this->delete($tmpl->withObject($obj));
                } elseif ($mode === self::EXACTMATCH_LITERAL) {
                    echo self::$debug > 1 ? "\tTurning <$sbj> skos:exactMatch <$obj> into literal\n" : '';
                    $this->forEach($asLiteral, $tmpl->withObject($obj));
                } elseif ($mode === self::EXACTMATCH_MERGE) {
                    echo self::$debug > 1 ? "\tMerging <$obj> into <$sbj>\n" : '';
                    $this->mergeConcepts($sbj, $obj);
                    $drop[] = $obj;
                }
            }
        }
        return array_filter($entities, fn(TermInterface $x) => !in_array((string) $x, $drop));
    }

    /**
     * 
     * @param array<TermInterface> $entities
     * @return void
     */
    private function assureTitles(array $entities): void {
        echo self::$debug ? "Processing titles...\n" : "";
        $titleSrcProps = array_map(fn($x) => DF::namedNode($x), $this->titleProperties);
        $titleProp     = $this->repo->getSchema()->label;
        foreach ($entities as $sbj) {
            if ($this->any(new QT($sbj, $titleProp))) {
                continue;
            }
            foreach ($titleSrcProps as $i) {
                $objs = iterator_to_array($this->listObjects(new QT($sbj, $i)));
                if (count($objs) > 0) {
                    break;
                }
            }
            if (count($objs ?? []) === 0) {
                $objs = [DF::literal((string) $sbj, 'und')];
            }
            foreach ($objs as $i) {
                if (!($i instanceof LiteralInterface)) {
                    $i = DF::literal((string) $i, 'und');
                }
                echo self::$debug > 1 ? "\tadding <$sbj> '$i'@" . $i->getLang() . "\n" : '';
                $this->add(DF::quad($sbj, $titleProp, $i));
            }
        }
    }

    /**
     * 
     * @param array<TermInterface> $entities
     * @return void
     */
    private function dropProperties(array $entities): void {
        if ($this->allowedNmsp === null) {
            return;
        }
        $schema      = $this->repo->getSchema();
        $allowedNmsp = [RDF::RDF_TYPE, $schema->id, $schema->label];
        $allowedNmsp = array_merge($allowedNmsp, $this->allowedNmsp);

        echo self::$debug ? "Removing properties outside of allowed namespaces...\n" : "";
        foreach ($entities as $sbj) {
            foreach ($this->listPredicates(new QT($sbj)) as $prop) {
                $delete  = true;
                $propStr = (string) $prop;
                foreach ($allowedNmsp as $nmsp) {
                    if (str_starts_with($propStr, $nmsp)) {
                        $delete = false;
                        break;
                    }
                }
                if ($delete) {
                    echo self::$debug > 1 ? "\tRemoving $prop from $sbj\n" : '';
                    $this->delete(new QT($sbj, $prop));
                }
            }
        }
    }

    /**
     * 
     * @param array<TermInterface> $entities
     * @return void
     */
    private function assureLiterals(array $entities): void {
        if ($this->allowedResourceNmsp === null) {
            return;
        }
        $schema    = $this->repo->getSchema();
        $allowed   = array_merge(
            [(string) $schema->id, $schema->parent, RDF::RDF_TYPE, RDF::NMSP_SKOS],
            $this->allowedResourceNmsp
        );
        $asLiteral = function (QuadInterface $x) {
            echo SkosVocabulary::$debug > 1 ? "\t<" . $x->getSubject() . "> <" . $x->getPredicate() . "> '" . $x->getObject() . "'\n" : '';
            return $x->withObject(DF::literal((string) $x->getObject(), null, RDF::XSD_ANY_URI));
        };
        echo self::$debug ? "Mapping resources to literals...\n" : "";
        foreach ($entities as $sbj) {
            foreach ($this->listPredicates(new QT($sbj)) as $prop) {
                foreach ($allowed as $i) {
                    if (str_starts_with($prop, $i)) {
                        continue 2;
                    }
                }
                $this->forEach($asLiteral, new QT($sbj, $prop, new NamedNodeTemplate(null, NamedNodeTemplate::ANY)));
            }
        }
    }

    /**
     * 
     * @param array<TermInterface> $entities
     * @return void
     */
    private function assureParents(array $entities): void {
        if (!$this->addParentProperty) {
            return;
        }
        $prop = $this->repo->getSchema()->parent;
        $url  = $this->vocabularyUrl;
        $this->add(array_map(fn($x) => DF::quad($x, $prop, $url), $entities));
    }

    /**
     * 
     * @param array<TermInterface> $entities
     * @return void
     */
    private function dropNodes(array $entities): void {
        echo self::$debug ? "Removing nodes not connected with the vocabulary...\n" : "";

        $valid = [];
        $queue = $entities;
        while (count($queue) > 0) {
            $sbj = array_pop($queue);
            if (!isset($valid[(string) $sbj])) {
                $valid[(string) $sbj] = $sbj;
                $queue                = array_merge(
                    $queue,
                    iterator_to_array($this->listObjects(new QT($sbj, null, new NamedNodeTemplate(null, NamedNodeTemplate::ANY))))
                );
            }
        }
        $removed = $this->deleteExcept(new QT(new AnyOfTemplate($valid)));
        echo self::$debug > 1 ? "$removed\n" : '';
    }

    private function mergeConcepts(TermInterface $into, TermInterface $res): void {
        $idProp    = $this->repo->getSchema()->id;
        $matchProp = DF::namedNode(RDF::SKOS_EXACT_MATCH);
        $matchTmpl = new PT($matchProp);

        // as skos:exactMatch is transitive, collect all concepts to be merged
        $mergeQueue = [$res];
        $mergedStr  = [(string) $into];
        while (count($mergeQueue) > 0) {
            $sbj         = array_pop($mergeQueue);
            $mergedStr[] = (string) $sbj;
            foreach ($this->listObjects($matchTmpl->withSubject($sbj)) as $obj) {
                if (!in_array((string) $obj, $mergedStr)) {
                    $mergeQueue[] = $obj;
                }
            }
        }
        array_shift($mergedStr); // remove the $into

        foreach ($mergedStr as $sbj) {
            $sbj = DF::namedNode($sbj);
            $this->delete(new QT($sbj));
            $this->add(DF::quad($into, $idProp, $sbj));
        }
    }

    /**
     * 
     * @param array<RepoResource|ClientException> $imported
     * @param int $concurrency
     * @param int $retriesOnConflict
     * @return void
     */
    private function removeObsolete(array $imported, int $concurrency = 3,
                                    int $retriesOnConflict = 3): void {
        echo self::$debug ? "Removing obsolete resources\n" : '';
        $importedUris      = array_map(fn($x) => $x instanceof RepoResource ? $x->getUri() : null, $imported);
        $schema            = $this->repo->getSchema();
        $term              = new SearchTerm([RDF::SKOS_IN_SCHEME, $schema->parent], $this->vocabularyUrl);
        $cfg               = new SearchConfig();
        $cfg->metadataMode = 'ids';
        $existing          = $this->repo->getResourcesBySearchTerms([$term], $cfg);
        $existing          = iterator_to_array($existing);
        $existingUris      = array_map(fn($x) => $x->getUri(), $existing);
        $existing          = array_combine($existingUris, $existing);
        $toRemove          = array_diff($existingUris, $importedUris);

        $debug = self::$debug;
        $f     = function (string $resUri, Repo $repo) use ($existing, $debug) {
            echo $debug > 1 ? "\tRemoving $resUri\n" : '';
            return $existing[$resUri]->deleteAsync(true);
        };
        while ($retriesOnConflict > 0 && count($toRemove) > 0) {
            $results = $this->repo->map($toRemove, $f, $concurrency, Repo::REJECT_INCLUDE);
            $tmp     = [];
            foreach ($results as $n => $i) {
                if ($i instanceof RejectedPromise) {
                    $tmp[] = $toRemove[$n];
                }
            }
            $toRemove = $tmp;
            $retriesOnConflict--;
        }
        if (count($toRemove) > 0) {
            throw new RepoLibException("Failed to remove all obsolete vocabulary children");
        }
    }
}
