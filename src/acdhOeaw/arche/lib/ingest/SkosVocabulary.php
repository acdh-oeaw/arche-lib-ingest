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
use EasyRdf\Literal;
use EasyRdf\Resource;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\SearchConfig;
use acdhOeaw\arche\lib\SearchTerm;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\RepoResourceInterface as RRI;

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

    static private $skosRelations = [
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
        $tmpFile = tempnam(sys_get_temp_dir(), 'skosvocab');
        $client  = new Client();
        $headers = ['Accept' => 'text/turtle;q=1, application/rdf+xml;q=0.8, application/n-triples;q=0.6, application/ld+json;q=0.4'];
        $resp    = $client->send(new Request('get', $url, $headers));
        $format  = $resp->getHeader('Content-Type')[0] ?? null;
        file_put_contents($tmpFile, (string) $resp->getBody());
        try {
            return new self($repo, $tmpFile, $format, $url);
        } finally {
            unlink($tmpFile);
        }
    }

    private string $file;
    private string $format;
    private string $vocabularyUrl;
    private string $state;

    /**
     * 
     * @var array<string>|null
     */
    private ?array $allowedNmsp = null;

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
     * Should all properties other then id, rdf:class, and skos relations be
     * casted to literals?
     */
    private bool $enforceLiterals = true;

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
        $this->file = tempnam(sys_get_temp_dir(), 'skosvocab');
        copy($file, $this->file);

        // make sure the RDF graph contains exactly one vocabulary
        $schemas = $this->allOfType(RDF::SKOS_CONCEPT_SCHEMA);
        if (count($schemas) === 0) {
            throw new RuntimeException("No skos:ConceptSchema found in the RDF graph");
        }
        if (count($schemas) > 1) {
            throw new RuntimeException("Many skos:ConceptSchema found in the RDF graph");
        }
        $this->vocabularyUrl = $schemas[0]->getUri();
        if (!empty($uri)) {
            $this->resource($this->vocabularyUrl)->addResource($schema->id, $uri);
        }

        // check if the vocabulary is up to date
        $repoRes     = $hash        = $repoResHash = null;
        try {
            $repoRes     = $this->repo->getResourceById($this->vocabularyUrl);
            $repoRes->loadMetadata(false, RRI::META_RESOURCE, null, [$schema->hash]);
            list($hashName, $repoResHash) = explode(':', $repoRes->getGraph()->getLiteral($schema->hash));
            $hash        = hash_init($hashName);
            hash_update_file($hash, $file);
            $hash        = hash_final($hash, false);
            $this->state = $hash === $repoResHash ? self::STATE_OK : self::STATE_UPDATE;
        } catch (NotFound $ex) {
            $this->state = self::STATE_NEW;
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
     * Set RDF property filter for skos resources
     * 
     * @param array<string>|null $nmsp null allows all properties
     * @return self
     */
    public function setAllowedNamespaces(?array $nmsp): self {
        $this->allowedNmsp = $nmsp;
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
     * When $enforce is set to true, all RDF properties of skos entities but 
     * skos properties, repository id property and repository parent property
     * are casted to literals.
     * 
     * This allows to avoid creation of unnecessary repository resources but
     * can lead to resulting data being incompatible with ontologies they were
     * following (as datatype and object properties are mutually exclusive in 
     * owl) which may or might be a problem for you.
     * 
     * @param bool $enforce
     * @return self
     */
    public function setEnforceLiterals(bool $enforce): self {
        $this->enforceLiterals = $enforce;
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
     * @param array $properties
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

        // find all concepts in the SKOS scheme
        $concepts    = $this->resourcesMatching(RDF::SKOS_IN_SCHEME, $this->resource($this->vocabularyUrl));
        $concepts    = array_map(fn($x) => $x->getUri(), $concepts);
        $collections = [];
        if ($this->importCollections) {
            $collections = [
                $this->resourcesMatching(RDF::RDF_TYPE, $this->resource(RDF::SKOS_COLLECTION)),
                $this->resourcesMatching(RDF::RDF_TYPE, $this->resource(RDF::SKOS_ORDERED_COLLECTION))
            ];
            $collections = array_unique(array_map(fn($x) => $x->getUri(), array_merge($collections[0], $collections[1])));
        }

        // preprocess
        $concepts            = $this->processExactMatches($concepts);
        $entities            = array_merge([$this->vocabularyUrl], $concepts);
        $entitiesCollections = array_merge($entities, $collections);

        $this->processRelations($entities);
        $this->assureTitles($entitiesCollections);
        $this->dropProperties($entitiesCollections);
        $this->assureLiterals($entitiesCollections);
        $this->assureParents($entitiesCollections);
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

        // perform the inestion
        $imported = parent::import($namespace, $singleOutNmsp, $errorMode, $concurrency, $retriesOnConflict);

        // upload vocabulary binary
        echo self::$debug ? "Uploading vocabulary binary\n" : '';
        $repoRes = $this->repo->getResourceById($this->vocabularyUrl);
        $payload = new BinaryPayload(null, $this->file, $this->format ?? null);
        echo self::$debug ? "Updating " . $repoRes->getUri() . "\n" : '';
        $repoRes->updateContent($payload, RRI::META_NONE);

        // remove obsolete entities
        $this->removeObsolete();

        return $imported;
    }

    /**
     * 
     * @param array<string> $entities
     * @return void
     */
    private function processRelations(array $entities): void {
        if ($this->relationsMode === self::RELATIONS_KEEP && $this->relationsModeSchema === self::RELATIONS_KEEP) {
            return;
        }
        echo self::$debug ? "Processing skos relations...\n" : "";
        foreach ($entities as $resUri) {
            $res   = $this->resource($resUri);
            $props = array_intersect(self::$skosRelations, $res->propertyUris());
            foreach ($props as $prop) {
                foreach ($res->allResources($prop) as $obj) {
                    /** @var Resource $obj */
                    $mode = in_array($obj->getUri(), $entities) ? $this->relationsModeSchema : $this->relationsMode;
                    if ($mode === self::RELATIONS_DROP) {
                        echo self::$debug > 1 ? "\tRemoving <" . $res->getUri() . "> <$prop> <" . $obj->getUri() . ">\n" : '';
                        $res->delete($prop, $obj);
                    } elseif ($mode === self::RELATIONS_LITERAL) {
                        echo self::$debug > 1 ? "\tTurning <" . $res->getUri() . "> <$prop> <" . $obj->getUri() . "> into literal\n" : '';
                        $res->delete($prop, $obj);
                        $res->add($prop, new Literal($obj->getUri(), null, RDF::XSD_ANY_URI));
                    }
                }
            }
        }
    }

    /**
     * 
     * @param array<string> $entities
     * @return array<string>
     */
    private function processExactMatches(array $entities): array {
        if ($this->exactMatchMode === self::EXACTMATCH_KEEP && $this->exactMatchModeSchema === self::EXACTMATCH_KEEP) {
            return $entities;
        }

        $drop = [];
        echo self::$debug ? "Processing skos:exactMatch...\n" : "";
        foreach ($entities as $resUri) {
            $res = $this->resource($resUri);
            foreach ($res->allResources(RDF::SKOS_EXACT_MATCH) as $obj) {
                $mode = in_array($obj->getUri(), $entities) ? $this->exactMatchModeSchema : $this->exactMatchMode;
                if ($mode === self::EXACTMATCH_DROP) {
                    echo self::$debug > 1 ? "\tRemoving <" . $res->getUri() . "> skos:exactMatch <" . $obj->getUri() . ">\n" : '';
                    $res->delete(RDF::SKOS_EXACT_MATCH, $obj);
                } elseif ($mode === self::EXACTMATCH_LITERAL) {
                    echo self::$debug > 1 ? "\tTurning <" . $res->getUri() . "> skos:exactMatch <" . $obj->getUri() . "> into literal\n" : '';
                    $res->delete(RDF::SKOS_EXACT_MATCH, $obj);
                    $res->add(RDF::SKOS_EXACT_MATCH, new Literal($obj->getUri(), null, RDF::XSD_ANY_URI));
                } elseif ($mode === self::EXACTMATCH_MERGE) {
                    echo self::$debug > 1 ? "\tMerging <" . $obj->getUri() . "> into <" . $res->getUri() . ">\n" : '';
                    $this->mergeConcepts($res, $obj);
                    $drop[] = $obj->getUri();
                }
            }
        }
        return array_diff($entities, $drop);
    }

    /**
     * 
     * @param array<string> $entities
     * @return void
     */
    private function assureTitles(array $entities): void {
        if ($this->titleProperties === null) {
            return;
        }
        echo self::$debug ? "Processing titles...\n" : "";
        $titleProp = $this->repo->getSchema()->label;
        foreach ($entities as $resUri) {
            $res = $this->resource($resUri);
            if ($res->getLiteral($titleProp) !== null) {
                continue;
            }
            foreach ($this->titleProperties as $prop) {
                $added = false;
                foreach ($res->all($prop) as $i) {
                    if (!($i instanceof Literal)) {
                        $i = new Literal((string) $i, 'und');
                    }
                    echo self::$debug > 1 ? "\tadding <" . $res->getUri() . "> '" . $i->getValue() . "'@" . $i->getLang() . "\n" : '';
                    $res->addLiteral($titleProp, $i);
                    $added = true;
                }
                if ($added) {
                    break;
                }
            }
        }
    }

    /**
     * 
     * @param array<string> $entities
     * @return void
     */
    private function dropProperties(array $entities): void {
        if ($this->allowedNmsp === null) {
            return;
        }
        $allowedNmsp = [RDF::RDF_TYPE, $this->repo->getSchema()->id];
        $allowedNmsp = array_merge($allowedNmsp, $this->allowedNmsp);

        echo self::$debug ? "Removing properties outside of allowed namespaces...\n" : "";
        foreach ($entities as $resUri) {
            $res = $this->resource($resUri);
            foreach ($res->propertyUris() as $prop) {
                $delete = true;
                foreach ($allowedNmsp as $nmsp) {
                    if (str_starts_with($prop, $nmsp)) {
                        $delete = false;
                        break;
                    }
                }
                if ($delete) {
                    echo self::$debug > 1 ? "\tRemoving $prop from " . $res->getUri() . "\n" : '';
                    $res->delete($prop);
                }
            }
        }
    }

    /**
     * 
     * @param array<string> $entities
     * @return void
     */
    private function assureLiterals(array $entities): void {
        if (!$this->enforceLiterals) {
            return;
        }
        $schema  = $this->repo->getSchema();
        $allowed = [$schema->id, $schema->parent, RDF::RDF_TYPE];
        echo self::$debug ? "Mapping resources to literals...\n" : "";
        foreach ($entities as $resUri) {
            $res = $this->resource($resUri);
            foreach ($res->propertyUris() as $prop) {
                if (in_array($prop, $allowed) || str_starts_with($prop, self::NMSP_SKOS)) {
                    continue;
                }
                foreach ($res->allResources($prop) as $i) {
                    echo self::$debug > 1 ? "\t<" . $res->getUri() . "> <$prop> '$i'\n" : '';
                    $res->addLiteral($prop, new Literal($i->getUri(), null, RDF::XSD_ANY_URI));
                }
                $res->deleteResource($prop);
            }
        }
    }

    private function assureParents(array $entities): void {
        if (!$this->addParentProperty) {
            return;
        }
        $parentProp = $this->repo->getSchema()->parent;
        foreach ($entities as $resUri) {
            $res = $this->resource($resUri);
            $res->addResource($parentProp, $this->vocabularyUrl);
        }
    }

    /**
     * 
     * @param array<string> $entities
     * @return void
     */
    private function dropNodes(array $entities): void {
        echo self::$debug ? "Removing nodes not connected with the vocabulary...\n" : "";

        $valid = $entities;
        $queue = $entities;
        while (count($queue) > 0) {
            $res = $this->resource(array_pop($queue));
            foreach ($res->propertyUris() as $prop) {
                foreach ($res->allResources($prop) as $obj) {
                    $obj = $obj->getUri();
                    if (!in_array($obj, $valid)) {
                        $valid[] = $obj;
                        $queue[] = $obj;
                    }
                }
            }
        }
        $toDrop = array_map(fn($x) => $x->getUri(), $this->resources());
        $toDrop = array_diff($toDrop, $valid);
        foreach ($toDrop as $resUri) {
            echo self::$debug > 1 ? "\t$res\n" : '';
            $res = $this->resource($resUri);
            // there's no need to care about reverse properties as resources
            // to delete form a separate subgraph
            foreach ($res->propertyUris() as $prop) {
                $res->delete($prop);
            }
        }
    }

    private function mergeConcepts(Resource $into, Resource $res): void {
        $idProp = $this->repo->getSchema()->id;

        // as skos:exactMatch is transitive, collect all concepts to be merged
        $toMerge = [$res->getUri()];
        $queue   = $toMerge;
        while (count($queue) > 0) {
            $res = $this->resource(array_pop($queue));
            foreach ($res->allResources(RDF::SKOS_EXACT_MATCH) as $obj) {
                $obj = $obj->getUri();
                if ($obj !== $into->getUri() && !in_array($obj, $toMerge)) {
                    $queue[]   = $obj;
                    $toMerge[] = $obj;
                }
            }
        }

        foreach ($toMerge as $resUri) {
            $into->deleteResource(RDF::SKOS_EXACT_MATCH, $resUri);
            $into->addResource($idProp, $resUri);
            $res = $this->resource($resUri);
            foreach ($res->propertyUris() as $prop) {
                $res->delete($prop);
            }
        }
    }

    /**
     * 
     * @param array<RepoResource> $imported
     * @return void
     */
    private function removeObsolete(array $imported): void {
        echo self::$debug ? "Removing obsolete resources\n" : '';
        $importedUris      = array_map(fn($x) => $x instanceof RepoResource ? $x->getUri() : null, $imported);
        $schema            = $this->repo->getSchema();
        $term              = new SearchTerm([RDF::SKOS_IN_SCHEME, $schema->parent], $this->vocabularyUrl);
        $cfg               = new SearchConfig();
        $cfg->metadataMode = 'ids';
        $existing          = iterator_to_array($this->repo->getResourcesBySearchTerms([
                $term], $cfg));
        $existingUris      = array_map(fn($x) => $x->getUri(), $existing);
        $existing          = array_combine($existingUris, $existing);
        $toRemove          = array_diff($existingUris, $importedUris);
        foreach ($toRemove as $resUri) {
            echo self::$debug > 1 ? "\tRemoving $resUri\n" : '';
            $existing[$resUri]->delete(true);
        }
    }
}
