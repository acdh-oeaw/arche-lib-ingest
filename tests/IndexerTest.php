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

use EasyRdf\Graph;
use EasyRdf\Literal;
use zozlak\RdfConstants as RDF;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\ClientException;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\Conflict;
use acdhOeaw\arche\lib\ingest\Indexer;
use acdhOeaw\arche\lib\ingest\IndexerException;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupFile;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupGraph;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupConstant;

/**
 * Description of IndexerTest
 *
 * @author zozlak
 */
class IndexerTest extends TestBase {

    const URI_PREFIX = 'acdhContainer://';

    static private RepoResource $res;

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

//        MetaLookupFile::$debug  = true;
//        MetaLookupGraph::$debug = true;
//        Indexer::$debug         = true;

        self::$repo->begin();
        $id = 'http://my.test/id';
        try {
            self::$res = self::$repo->getResourceById($id);
        } catch (NotFound $ex) {
            $meta      = (new Graph())->resource('.');
            $meta->addLiteral(self::$config->schema->label, 'test parent');
            $meta->addLiteral(self::$config->schema->fileName, 'data');
            $meta->addResource(self::$config->schema->id, $id);
            self::$res = self::$repo->createResource($meta);
        }
        self::$repo->commit();
    }

    static public function tearDownAfterClass(): void {
        parent::tearDownAfterClass();

        if (isset(self::$res)) {
            self::$repo->begin();
            self::$res->delete(true, true);
            self::$repo->commit();
        }
    }

    private Indexer $ind;
    private string $tmpDir = __DIR__ . '/tmp/';
    private string $tmpContent;

    public function setUp(): void {
        parent::setUp();
        $this->ind = new Indexer(__DIR__ . '/data', self::URI_PREFIX, self::$repo);
        $this->ind->setDepth(1);
        $this->ind->setParent(self::$res);
        $this->ind->setUploadSizeLimit(10000000);
        if (file_exists($this->tmpDir)) {
            system("rm -fR " . $this->tmpDir);
        }
        $this->tmpContent = file_get_contents(__DIR__ . '/data/sample.xml');
    }

    public function tearDown(): void {
        parent::tearDown();
        if (file_exists($this->tmpDir)) {
            system("rm -fR " . $this->tmpDir);
        }
        file_put_contents(__DIR__ . '/data/sample.xml', $this->tmpContent);
    }

    /**
     * @group indexer
     */
    public function testSimple(): void {
        self::$test = 'testSimple';

        $this->ind->setFilter('/txt|xml/', Indexer::FILTER_MATCH);
        $this->ind->setFilter('/^(skiptest.txt)$/', Indexer::FILTER_SKIP);
        self::$repo->begin();
        $indRes = $this->ind->import(Indexer::ERRMODE_FAIL);
        $this->noteResources($indRes);
        foreach ($indRes as $i) {
            $file = (string) $i->getGraph()->getLiteral(self::$config->schema->fileName);
            if (is_file(__DIR__ . "/data/$file")) {
                $resp = self::$repo->sendRequest(new Request('get', $i->getUri()));
                $this->assertEquals(file_get_contents(__DIR__ . "/data/$file"), (string) $resp->getBody());
            }
        }
        
        self::$repo->commit();
        foreach ($indRes as $i) {
            $file = (string) $i->getGraph()->getLiteral(self::$config->schema->fileName);
            if (is_file(__DIR__ . "/data/$file")) {
                $resp = self::$repo->sendRequest(new Request('get', $i->getUri()));
                $this->assertEquals(file_get_contents(__DIR__ . "/data/$file"), (string) $resp->getBody());
            }
        }

        $this->assertEquals(6, count($indRes));
    }

    /**
     * @group indexer
     */
    public function testSkipNotExist(): void {
        self::$test = 'testSkipNotExist';

        $this->testSimple();

        $this->ind->setFilter('', Indexer::FILTER_SKIP);
        $this->ind->setSkip(Indexer::SKIP_NOT_EXIST);
        self::$repo->begin();
        $indRes = $this->ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(6, count($indRes));
    }

    /**
     * @group indexer
     */
    public function testSkipExist(): void {
        self::$test = 'testSkipExist';

        $indRes1 = $indRes2 = [];

        $this->ind->setFilter('/txt/', Indexer::FILTER_MATCH);
        self::$repo->begin();
        $indRes1 = $this->ind->import();
        $this->noteResources($indRes1);
        self::$repo->commit();

        $this->assertEquals(4, count($indRes1));

        $this->ind->setSkip(Indexer::SKIP_EXIST);
        $this->ind->setFilter('/(txt|xml)$/', Indexer::FILTER_MATCH);
        self::$repo->begin();
        $indRes2 = $this->ind->import();
        $this->noteResources($indRes2);
        self::$repo->commit();

        $this->assertEquals(2, count($indRes2));
    }

    /**
     * @group indexer
     * @group indexerSkip
     */
    public function testSkipBinaryExist(): void {
        self::$test = 'testSkipBinaryExist';

        $indRes1 = $indRes2 = [];

        $this->ind->setFilter('/txt/', Indexer::FILTER_MATCH);
        self::$repo->begin();
        $indRes1 = $this->ind->import();
        $this->noteResources($indRes1);
        self::$repo->commit();

        $this->assertEquals(4, count($indRes1));

        $this->ind->setSkip(Indexer::SKIP_BINARY_EXIST);
        $this->ind->setFilter('/(txt|xml)$/', Indexer::FILTER_MATCH);
        self::$repo->begin();
        $indRes2 = $this->ind->import();
        $this->noteResources($indRes2);
        self::$repo->commit();

        $this->assertEquals(4, count($indRes2));
    }

    /**
     * @group indexer
     */
    public function testMetaFromFile(): void {
        self::$test = 'testMetaFromFile';

        $metaLookup = new MetaLookupFile(['.'], '.ttl');
        $this->ind->setDepth(0);
        $this->ind->setMetaLookup($metaLookup);
        $this->ind->setFilter('/sample.xml$/');
        self::$repo->begin();
        $indRes     = $this->ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    /**
     * @group indexer
     */
    public function testMetaFromGraph(): void {
        self::$test = 'testMetaFromGraph';

        $graph      = new Graph();
        $graph->parseFile(__DIR__ . '/data/sample.xml.ttl');
        $metaLookup = new MetaLookupGraph($graph, self::$repo->getSchema()->id);
        $this->ind->setDepth(0);
        $this->ind->setMetaLookup($metaLookup);
        $this->ind->setFilter('/sample.xml$/');
        self::$repo->begin();
        $indRes     = $this->ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    /**
     * @group indexer
     */
    public function testSkipWithoutMetaInFile(): void {
        self::$test = 'testSkipWithoutMetaInFile';

        $metaLookup = new MetaLookupFile(['.'], '.ttl');
        self::$repo->begin();
        $this->ind->setMetaLookup($metaLookup, true);
        $this->ind->setFilter('/xml$/');
        $indRes     = $this->ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    /**
     * @group indexer
     */
    public function testWithoutMetaInGraph(): void {
        self::$test = 'testWithoutMetaInGraph';

        $metaLookup = new MetaLookupFile(['.'], '.ttl');
        self::$repo->begin();
        $this->ind->setMetaLookup($metaLookup, true);
        $this->ind->setFilter('/sample.xml$/');
        $indRes     = $this->ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    /**
     * @group indexer
     */
    public function testMergeOnExtMeta(): void {
        self::$test = 'testMergeOnExtMeta';

        $idProp    = self::$repo->getSchema()->id;
        $titleProp = self::$repo->getSchema()->label;
        $commonId  = 'https://my.id.nmsp/' . rand();
        $fileName  = rand();

        self::$repo->begin();

        $meta = (new Graph())->resource('.');
        $meta->addResource($idProp, $commonId);
        $meta->addLiteral($titleProp, 'sample title');
        $res1 = self::$repo->createResource($meta);
        $this->noteResources([$res1]);

        $meta->delete($titleProp);
        mkdir($this->tmpDir);
        file_put_contents($this->tmpDir . $fileName . '.ttl', $meta->getGraph()->serialise('turtle'));
        file_put_contents($this->tmpDir . $fileName, 'sample content');
        $ind    = new Indexer($this->tmpDir, self::URI_PREFIX, self::$repo);
        $ind->setFilter('/^' . $fileName . '$/');
        $ind->setMetaLookup(new MetaLookupFile(['.'], '.ttl'));
        $indRes = $ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $this->assertEquals($res1->getUri(), array_pop($indRes)->getUri());
    }

    /**
     * @group indexer
     */
    public function testMergeAndDeleted(): void {
        self::$test = 'testMergeAndDeleted';

        $idProp    = self::$repo->getSchema()->id;
        $titleProp = self::$repo->getSchema()->label;
        $commonId  = 'https://my.id.nmsp/' . rand();
        $fileName  = rand();

        //first instance of a resource created in a separate transaction
        self::$repo->begin();
        $meta = (new Graph())->resource('.');
        $meta->addResource($idProp, $commonId);
        $meta->addLiteral($titleProp, 'sample title');
        $res2 = self::$repo->createResource($meta);
        $this->noteResources([$res2]);
        self::$repo->commit();

        // main transaction
        self::$repo->begin();
        $res2->delete(true);
        $res3 = self::$repo->createResource($meta);
        $this->noteResources([$res3]);
        $res3->delete(true);
        $res4 = self::$repo->createResource($meta);
        $this->noteResources([$res4]);

        // preparare files on a disk
        $meta->delete($titleProp);
        mkdir($this->tmpDir);
        file_put_contents($this->tmpDir . $fileName . '.ttl', $meta->getGraph()->serialise('turtle'));
        file_put_contents($this->tmpDir . $fileName, 'sample content');
        // index
        $ind    = new Indexer($this->tmpDir, self::URI_PREFIX, self::$repo);
        $ind->setFilter('/^' . $fileName . '$/');
        $ind->setMetaLookup(new MetaLookupFile(['.'], '.ttl'));
        $indRes = $ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        // indexed resource should match manually created one
        $this->assertEquals(1, count($indRes));
        $this->assertEquals($res4->getUri(), array_pop($indRes)->getUri());
    }

    /**
     * @group indexer
     */
    public function testConflict1(): void {
        self::$test = 'testConflict1';

        $meta = (new Graph())->resource('.');
        $meta->addResource(self::$config->schema->id, 'http://foo/' . rand());

        $this->ind->setFilter('/txt|xml/', Indexer::FILTER_MATCH);
        $this->ind->setFlatStructure(true);
        $this->ind->setMetaLookup(new MetaLookupConstant($meta));
        self::$repo->begin();
        $indRes = $this->ind->import(Indexer::ERRMODE_FAIL);
        $this->noteResources($indRes);
        self::$repo->commit();
        $this->assertEquals(5, count($indRes));
    }

    /**
     * Like testConflict1 but intended to fail because reingestion is turned off
     * 
     * @group indexer
     */
    public function testConflict2(): void {
        self::$test = 'testConflict2';

        $meta = (new Graph())->resource('.');
        $meta->addResource(self::$config->schema->id, 'http://foo/' . rand());

        $this->ind->setFilter('/txt|xml/', Indexer::FILTER_MATCH);
        $this->ind->setFlatStructure(true);
        $this->ind->setMetaLookup(new MetaLookupConstant($meta));
        self::$repo->begin();
        try {
            $indRes = $this->ind->import(Indexer::ERRMODE_FAIL, 3, 0);
            $this->noteResources($indRes);
            $this->assertTrue(false);
        } catch (IndexerException $e) {
            $this->assertInstanceOf(Conflict::class, $e->getPrevious());
        }
        self::$repo->commit();
    }

    /**
     * @group indexer
     */
    public function testAutocommit(): void {
        self::$test = 'testAutocommit';

        self::$repo->begin();
        $this->ind->setFilter('/txt|xml/');
        $this->ind->setAutoCommit(2);
        $indRes = $this->ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();
        $this->assertEquals(7, count($indRes));
    }

    /**
     * @group indexer
     */
    public function testNewVersionCreation(): void {
        self::$test = 'testNewVersionCreation';

        $pidProp = self::$repo->getSchema()->ingest->pid;
        $pid     = 'https://sample.pid/' . rand();

        $indRes1 = $indRes2 = $indRes3 = [];
        $this->ind->setFilter('/^sample.xml$/', Indexer::FILTER_MATCH);
        $this->ind->setFlatStructure(true);

        self::$repo->begin();
        $indRes1 = $this->ind->import();
        $this->noteResources($indRes1);
        $initRes = array_pop($indRes1);
        $meta    = $initRes->getMetadata();
        $meta->addResource($pidProp, $pid);
        $initRes->setMetadata($meta);
        $initRes->updateMetadata();
        self::$repo->commit();

        file_put_contents(__DIR__ . '/data/sample.xml', random_int(0, 123456));

        self::$repo->begin();
        $this->ind->setVersioning(Indexer::VERSIONING_DIGEST, Indexer::PID_PASS);
        $indRes2 = $this->ind->import();
        $this->noteResources($indRes2);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes2));
        $newRes    = array_pop($indRes2);
        $meta      = $newRes->getMetadata();
        $this->assertEquals($pid, (string) $meta->getResource($pidProp)); // PID copied to the new resource - depends on the repo recognizing pid property as a non-relation one
        $this->assertTrue(in_array($pid, $newRes->getIds())); // depends on PID being copied to id (which is NOT the default repository setup cause the repository doesn't know the PID concept)
        $prevResId = (string) $meta->getResource(self::$repo->getSchema()->isNewVersionOf);
        $this->assertTrue(!empty($prevResId));
        $prevRes   = self::$repo->getResourceById($prevResId);
        $prevMeta  = $prevRes->getMetadata();
        $this->assertNull($prevMeta->getResource($pidProp)); // PID not present in the old resource

        file_put_contents(__DIR__ . '/data/sample.xml', random_int(0, 123456));

        self::$repo->begin();
        $this->ind->setVersioning(Indexer::VERSIONING_DIGEST, Indexer::PID_KEEP);
        $indRes3 = $this->ind->import();
        $this->noteResources($indRes3);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes3));
        $newestRes  = array_pop($indRes3);
        $newestMeta = $newestRes->getMetadata();
        $this->assertNull($newestMeta->getResource($pidProp));
        $newMeta    = $newRes->getMetadata();
        $this->assertEquals($pid, (string) $newMeta->getResource($pidProp));
        $this->assertTrue(in_array($pid, $newRes->getIds()));
    }

    /**
     * @group indexer
     */
    public function testErrMode(): void {
        self::$test = 'testFailure';
        $prop       = self::$config->schema->label;
        self::$repo->begin();

        $meta = (new Graph())->resource('.');
        $meta->addLiteral($prop, new Literal('foo', null, RDF::XSD_DATE));
        $this->ind->setMetaLookup(new MetaLookupConstant($meta));
        $this->ind->setDepth(0);
        $this->ind->setAutoCommit(2);
        $this->ind->setFilter('/xml/', Indexer::FILTER_MATCH);

        // ERRMODE_FAIL
        try {
            $this->ind->import(Indexer::ERRMODE_FAIL);
            $this->assertTrue(false);
        } catch (IndexerException $e) {
            $this->assertStringContainsString('Wrong property value', $e->getPrevious()->getMessage());
            $this->assertCount(0, $e->getCommitedResources());
        }

        // ERRMODE_PASS
        try {
            $this->ind->import(Indexer::ERRMODE_PASS);
        } catch (IndexerException $e) {
            $this->assertStringContainsString('There was at least one error during the import', $e->getMessage());
            $this->assertStringContainsString('Wrong property value', $e->getMessage());
            $this->assertCount(0, $e->getCommitedResources());
        }

        // ERRMODE_INCLUDE
        $processed = $this->ind->import(Indexer::ERRMODE_INCLUDE);
        $this->assertCount(3, $processed);
        foreach ($processed as $i) {
            $this->assertInstanceOf(ClientException::class, $i);
            $this->assertStringContainsString('Wrong property value', (string) $i->getResponse()->getBody());
        }

        self::$repo->rollback();
    }

    /**
     * 
     * @large
     * @group largeIndexer
     */
    public function testRealWorldData(): void {
        self::$test = 'testRealWorldData';

        $this->ind->setFilter('/.*/');
        $this->ind->setDepth(100);
        self::$repo->begin();
        $indRes = $this->ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();
        $this->assertEquals(83, count($indRes));
    }

    /**
     * 
     * @large
     */
    public function testBigFile(): void {
        self::$test = 'testBigFile';

        $bufLen = 1024 * 1024;
        $buf    = str_repeat('a', $bufLen); // 1 MB
        $count  = 1024; // 1 GB

        mkdir($this->tmpDir);
        $f = fopen($this->tmpDir . '/test', 'wb');
        for ($i = 0; $i < $count; $i++) {
            fwrite($f, $buf);
        }
        fclose($f);
        unset($buf);

        $ind    = new Indexer($this->tmpDir, self::URI_PREFIX, self::$repo);
        $ind->setUploadSizeLimit($count * $bufLen);
        self::$repo->begin();
        $indRes = $ind->import();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $this->assertEquals($count * $bufLen, (int) array_pop($indRes)->getMetadata()->getLiteral(self::$repo->getSchema()->binarySize)->getValue());
    }
}
