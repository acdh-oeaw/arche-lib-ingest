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

namespace acdhOeaw\arche\lib\ingest\tests;

use GuzzleHttp\Exception\ClientException;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\ingest\IndexerException;
use acdhOeaw\arche\lib\ingest\MetadataCollection;

/**
 * Description of MetadataCollectionTest
 *
 * @author zozlak
 */
class MetadataCollectionTest extends TestBase {

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        //MetadataCollection::$debug = true;
    }

    /**
     * @group metadataCollection
     */
    public function testSimple(): void {
        self::$test = 'testSimple';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-small.ttl');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(2, count($indRes));
    }

    /**
     * @group metadataCollection
     */
    public function testSimpleDouble(): void {
        self::$test = 'testSimpleDouble';

        $graph   = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-small.ttl');
        self::$repo->begin();
        $indRes1 = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes1);
        self::$repo->commit();

        $this->assertEquals(2, count($indRes1));

        $graph   = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-small.ttl');
        self::$repo->begin();
        $indRes2 = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes2);
        self::$repo->commit();

        $this->assertEquals(2, count($indRes2));
        $r1u = $r2u = [];
        foreach ($indRes1 as $i) {
            $r1u[] = $i->getUri();
        }
        foreach ($indRes2 as $i) {
            $r2u[] = $i->getUri();
        }
        $this->assertEquals(count($r1u), count($r2u));
        $this->assertEquals([], array_diff($r2u, $r1u));
    }

    /**
     * @group metadataCollection
     */
    public function testLarge(): void {
        self::$test = 'testLarge';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-large.ttl');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(19, count($indRes));
    }

    /**
     * @group metadataCollection
     */
    public function testCycle(): void {
        self::$test = 'testCycle';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-cycle.ttl');
        self::$repo->begin();
        $indRes = $graph->import('http://some.id', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(2, count($indRes));
    }

    /**
     * @group metadataCollection
     */
    public function testBNodes(): void {
        self::$test = 'testBNodes';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/bnodes.ttl');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(2, count($indRes));
    }

    /**
     * @group metadataCollection
     */
    public function testAutoRefsCreation(): void {
        self::$test = 'testAutoRefsCreation';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-autorefs.ttl');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(3, count($indRes));
    }

    /**
     * @group metadataCollection
     */
    public function testBasicResources(): void {
        self::$test = 'testBasicResources';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/basicResources.ttl');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(14, count($indRes));
        $acdh     = self::$repo->getResourceById('https://viaf.org/viaf/6515148451584915970000');
        $acdhMeta = $acdh->getMetadata();
        $this->assertEquals(1010, (int) $acdhMeta->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasPostcode')->getValue());
        $this->assertEquals('Austrian Centre for Digital Humanities and Cultural Heritage', (string) $acdhMeta->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasTitle'));

        // repeat to make sure there are no issues with resource duplication, etc.
        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/basicResources.ttl');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(14, count($indRes));
    }

    /**
     * @large
     * @group largeMetadataCollection
     */
    public function testBig(): void {
        self::$test = 'testBig';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/schnitzler-diaries.rdf');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(75, count($indRes));
    }

    /**
     * @group metadataCollection
     */
    public function testImportSingleOutNmsp(): void {
        self::$test = 'testImportSingleOutNmsp';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/basicResources.ttl');
        self::$repo->begin();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('singleOutNmsp parameters must be one of MetadataCollection::SKIP, MetadataCollection::CREATE');
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', 99);
        $this->noteResources($indRes);
        self::$repo->commit();
    }

    /**
     * @group metadataCollection
     */
    public function testImportWrongErrorMode(): void {
        self::$test = 'testImportWrongErrorMode';

        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/basicResources.ttl');
        self::$repo->begin();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('errorMode parameters must be one of MetadataCollection::ERRMODE_FAIL and MetadataCollection::ERRMODE_PASS');
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP, 'none');
        $this->noteResources($indRes);
        self::$repo->commit();
    }

    /**
     * @group metadataCollection
     */
    public function testImportErrorMode(): void {
        self::$test = 'testImportErrorMode';

        // ERRMODE_INCLUDE
        $graph  = new MetadataCollection(self::$repo, __DIR__ . '/data/errmode.ttl');
        self::$repo->begin();
        $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP, MetadataCollection::ERRMODE_INCLUDE);
        self::$repo->rollback();

        $classes = [];
        foreach ($indRes as $i) {
            $classes[] = get_class($i);
        }
        $this->assertCount(2, $indRes);
        $this->assertCount(2, $classes);
        $this->assertContains(RepoResource::class, $classes);
        $this->assertContains(ClientException::class, $classes);

        // ERRMODE_PASS
        $graph = new MetadataCollection(self::$repo, __DIR__ . '/data/errmode.ttl');
        self::$repo->begin();
        try {
            $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP, MetadataCollection::ERRMODE_PASS);
            $this->assertTrue(false);
        } catch (IndexerException $e) {
            $this->assertStringStartsWith('There was at least one error during the import', $e->getMessage());
        }
        $this->assertInstanceOf(RepoResource::class, self::$repo->getResourceById('https://id.acdh.oeaw.ac.at/id1'));
        try {
            self::$repo->getResourceById('https://id.acdh.oeaw.ac.at/id2');
            $this->assertTrue(false);
        } catch (NotFound $e) {
            $this->assertTrue(true);
        }
        self::$repo->rollback();

        // ERRMODE_FAIL
        $graph = new MetadataCollection(self::$repo, __DIR__ . '/data/errmode.ttl');
        self::$repo->begin();
        try {
            $indRes = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP, MetadataCollection::ERRMODE_FAIL);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $this->assertStringContainsString('Wrong property value', $e->getMessage());
        }
        // can't test for resource one as ingestion order is unknown
        try {
            self::$repo->getResourceById('https://id.acdh.oeaw.ac.at/id2');
            $this->assertTrue(false);
        } catch (NotFound $e) {
            $this->assertTrue(true);
        }
        self::$repo->rollback();
    }
}
