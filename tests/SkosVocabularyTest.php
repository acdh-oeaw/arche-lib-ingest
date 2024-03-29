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

namespace acdhOeaw\arche\lib\ingest\tests;

use Exception;
use rdfInterface\QuadInterface;
use quickRdf\DataFactory as DF;
use termTemplates\PredicateTemplate as PT;
use termTemplates\QuadTemplate as QT;
use termTemplates\NotTemplate;
use termTemplates\NamedNodeTemplate;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\ingest\SkosVocabulary;
use zozlak\RdfConstants as RDF;

/**
 * Description of SkosVocabularyTest
 *
 * @author zozlak
 */
class SkosVocabularyTest extends TestBase {

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        //SkosVocabulary::$debug = 2;
    }

    /**
     * @group SkosVocabulary
     */
    public function testSimple(): void {
        self::$test = 'testSimple';

        $vocab    = new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl');
        $vocab->preprocess();
        self::$repo->begin();
        $imported = $vocab->import();
        self::$repo->commit();
        $this->noteResources($imported);
        $this->assertCount(3, $imported);

        $vocab = new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl');
        $this->assertEquals(SkosVocabulary::STATE_OK, $vocab->getState());
        $vocab->preprocess();
        $this->assertCount(0, $vocab->import());
    }

    /**
     * @group SkosVocabulary
     */
    public function testRemoveObsolete(): void {
        self::$test = 'removeObsolete';
        $schema     = self::$repo->getSchema();

        $vocab      = new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl');
        // make a copy of https://foo/scheme/3 as https://foo/scheme/9
        $newConcept = DF::namedNode('https://foo/scheme/9');
        $tmpl       = new QT(DF::namedNode('https://foo/scheme/3'), new NotTemplate($schema->id));
        $vocab->add($vocab->map(fn(QuadInterface $x) => $x->withSubject($newConcept), $tmpl));
        $vocab->preprocess();
        self::$repo->begin();
        $imported   = $vocab->import();
        self::$repo->commit();
        $this->noteResources($imported);
        $this->assertCount(4, $imported);

        $vocab    = new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl');
        $vocab->forceUpdate();
        $vocab->preprocess();
        self::$repo->begin();
        $imported = $vocab->import();
        self::$repo->commit();
        $this->noteResources($imported);
        $this->assertCount(3, $imported);

        try {
            self::$repo->getResourceById('https://foo/scheme/9');
            $this->assertTrue(false, "https://foo/scheme/9 hasn't been removed");
        } catch (NotFound $ex) {
            $this->assertTrue(true);
        }
    }

    /**
     * @group SkosVocabulary
     */
    public function testImportEverything(): void {
        self::$test = 'ingestEverything';

        $vocab    = (new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl'))
            ->setExactMatchMode(SkosVocabulary::EXACTMATCH_KEEP, SkosVocabulary::EXACTMATCH_KEEP)
            ->setSkosRelationsMode(SkosVocabulary::RELATIONS_KEEP, SkosVocabulary::RELATIONS_KEEP)
            ->setAllowedResourceNamespaces(null)
            ->setImportCollections(true)
            ->preprocess();
        self::$repo->begin();
        $imported = $vocab->import();
        self::$repo->commit();
        $this->noteResources($imported);
        $this->assertCount(7, $imported);
    }

    /**
     * @group SkosVocabulary
     */
    public function testImportMinimal(): void {
        self::$test = 'importMinimal';

        $schema     = self::$repo->getSchema();
        $titleProp  = $schema->label;
        $idProp     = $schema->label;
        $parentProp = $schema->parent;

        $vocab    = (new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl'))
            ->setExactMatchMode(SkosVocabulary::EXACTMATCH_DROP, SkosVocabulary::EXACTMATCH_DROP)
            ->setSkosRelationsMode(SkosVocabulary::RELATIONS_DROP, SkosVocabulary::RELATIONS_DROP)
            ->setAllowedResourceNamespaces([])
            ->setImportCollections(false)
            ->setAddParentProperty(false)
            ->setAllowedNamespaces([])
            ->setTitleProperties([])
            ->setAddTitle(true)
            ->preprocess();
        self::$repo->begin();
        $imported = $vocab->import();
        self::$repo->commit();
        $this->noteResources($imported);
        $this->assertCount(4, $imported);
        foreach ($imported as $i) {
            $meta = $i->getGraph();
            $this->assertCount(1, $meta->copy(new PT(DF::namedNode(RDF::RDF_TYPE))));
            $this->assertTrue($meta->none(new PT(DF::namedNode('http://purl.org/dc/elements/1.1/title'))));
            $this->assertTrue($meta->none(new PT($parentProp)));
            $ids  = $meta->listObjects(new PT($idProp))->getValues();
            $ids  = array_diff($ids, [(string) $i->getUri()]);
            $this->assertEquals($ids[0] ?? '__error__', (string) $meta->getObject(new PT($titleProp)));
        }
    }

    /**
     * @group SkosVocabulary
     */
    public function testFromUrl(): void {
        self::$test = 'fromUrl';
        $idProp     = self::$repo->getSchema()->id;
        $url        = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/rest/v1/arche_licenses/data');
        $vocab      = SkosVocabulary::fromUrl(self::$repo, $url);
        $this->assertCount(1, $vocab->copy(new PT($idProp, $url)));
        unset($vocab);
    }

    /**
     * @group SkosVocabulary
     */
    public function testSkosAsLiteral(): void {
        $objTmpl = new NamedNodeTemplate(null, NamedNodeTemplate::ANY);
        $vocab    = (new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl'))
            ->setExactMatchMode(SkosVocabulary::EXACTMATCH_LITERAL, SkosVocabulary::EXACTMATCH_DROP)
            ->setSkosRelationsMode(SkosVocabulary::RELATIONS_LITERAL, SkosVocabulary::RELATIONS_LITERAL);
        $vocab->preprocess();
        self::$repo->begin();
        $imported = $vocab->import();
        self::$repo->commit();
        $this->noteResources($imported);
        $this->assertCount(4, $imported);
        foreach ($imported as $i) {
            $meta = $i->getGraph();
            $this->assertTrue($meta->none(new PT(DF::namedNode(RDF::SKOS_EXACT_MATCH), $objTmpl)));
            $this->assertTrue($meta->none(new PT(DF::namedNode(RDF::SKOS_IN_SCHEME), $objTmpl)));
            if ($i->isA(RDF::SKOS_CONCEPT)) {
                $this->assertCount(1, $meta->copy(new PT(DF::namedNode(RDF::SKOS_IN_SCHEME))));
            }
        }
    }

    /**
     * @group SkosVocabulary
     */
    public function testErrors(): void {
        $tmpFile = tempnam(sys_get_temp_dir(), '');

        file_put_contents($tmpFile, '<https://foo/bar> <https://bar/baz> "foo" .');
        try {
            new SkosVocabulary(self::$repo, $tmpFile, 'application/n-triples');
            $this->assertTrue(false, 'No error on no skos:ConceptSchema');
        } catch (Exception $ex) {
            $this->assertEquals('No skos:ConceptSchema found in the RDF graph', $ex->getMessage());
        }

        $vocabulary = "
            <https://foo/bar> <" . RDF::RDF_TYPE . "> <" . RDF::SKOS_CONCEPT_SCHEMA . "> .
            <https://foo/baz> <" . RDF::RDF_TYPE . "> <" . RDF::SKOS_CONCEPT_SCHEMA . "> .
        ";
        file_put_contents($tmpFile, $vocabulary);
        try {
            new SkosVocabulary(self::$repo, $tmpFile, 'application/n-triples');
            $this->assertTrue(false, 'No error on many skos:ConceptSchema');
        } catch (Exception $ex) {
            $this->assertEquals('Many skos:ConceptSchema found in the RDF graph', $ex->getMessage());
        }

        $vocab = new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl');
        try {
            $vocab->setExactMatchMode('foo', 'bar');
            $this->assertTrue(false, 'Wrong exact match mode accepted');
        } catch (Exception $ex) {
            $this->assertEquals('Wrong inVocabulary or notInVocabulary parameter value', $ex->getMessage());
        }
        try {
            $vocab->setSkosRelationsMode('foo', 'bar');
            $this->assertTrue(false, 'Wrong skos relations mode accepted');
        } catch (Exception $ex) {
            $this->assertEquals('Wrong inVocabulary or notInVocabulary parameter value', $ex->getMessage());
        }
        unlink($tmpFile);
    }
}
