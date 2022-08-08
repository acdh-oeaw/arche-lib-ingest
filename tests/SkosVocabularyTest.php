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

/**
 * Description of SkosVocabularyTest
 *
 * @author zozlak
 */
class SkosVocabularyTest extends TestBase {

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        SkosVocabulary::$debug = 0;
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
}
