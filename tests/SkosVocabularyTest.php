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

        $vocab    = new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl');
        $vocab->resource('https://foo/scheme/3')->copy([$schema->id], '/^$/', 'https://foo/scheme/9', $vocab);
        $vocab->preprocess();
        self::$repo->begin();
        $imported = $vocab->import();
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
            ->setEnforceLiterals(false)
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
        $titleProp  = self::$repo->getSchema()->label;
        $idProp     = self::$repo->getSchema()->label;

        $vocab    = (new SkosVocabulary(self::$repo, __DIR__ . '/data/skosVocabulary.ttl'))
            ->setExactMatchMode(SkosVocabulary::EXACTMATCH_DROP, SkosVocabulary::EXACTMATCH_DROP)
            ->setSkosRelationsMode(SkosVocabulary::RELATIONS_DROP, SkosVocabulary::RELATIONS_DROP)
            ->setEnforceLiterals(true)
            ->setImportCollections(false)
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
            $this->assertCount(1, $meta->allResources(RDF::RDF_TYPE));
            $this->assertCount(0, $meta->all('http://purl.org/dc/elements/1.1/title'));
            $ids  = array_map(fn($x) => (string) $x, $meta->all($idProp));
            $ids  = array_diff($ids, [$i->getUri()]);
            $this->assertEquals($ids[0] ?? '__error__', (string) $meta->get($titleProp));
        }
    }

    /**
     * @group SkosVocabulary
     */
    public function testFromUrl(): void {
        self::$test = 'fromUrl';
        $idProp     = self::$repo->getSchema()->id;
        $url        = 'https://vocabs.acdh.oeaw.ac.at/rest/v1/arche_licenses/data';
        $vocab      = SkosVocabulary::fromUrl(self::$repo, $url);
        $this->assertCount(1, $vocab->resourcesMatching($idProp, $vocab->resource($url)));
        unset($vocab);
    }

    public function testErrors(): void {
        
    }
}
