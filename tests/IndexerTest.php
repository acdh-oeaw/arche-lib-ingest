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

use EasyRdf\Graph;
use acdhOeaw\acdhRepoIngest\metaLookup\MetaLookupFile;
use acdhOeaw\acdhRepoIngest\metaLookup\MetaLookupGraph;
use acdhOeaw\acdhRepoLib\exception\NotFound;

/**
 * Description of IndexerTest
 *
 * @author zozlak
 */
class IndexerTest extends TestBase {

    static private $res;

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

//        \acdhOeaw\acdhRepoIngest\schema\SchemaObject::$debug = true;
//        MetaLookupFile::$debug                               = true;
//        MetaLookupGraph::$debug                              = true;
//        Indexer::$debug = true;

        self::$repo->begin();
        $id = 'http://my.test/id';
        try {
            self::$res = self::$repo->getResourceById($id);
        } catch (NotFound $ex) {
            $meta      = (new Graph())->resource('.');
            $meta->addLiteral(self::$config->schema->label, 'test parent');
            $meta->addLiteral(self::$config->schema->ingest->location, 'data');
            $meta->addResource(self::$config->schema->id, $id);
            self::$res = self::$repo->createResource($meta);
        }
        self::$repo->commit();
    }

    static public function tearDownAfterClass(): void {
        parent::tearDownAfterClass();

        if (self::$res) {
            self::$repo->begin();
            self::$res->delete(true, true);
            self::$repo->commit();
        }
    }

    private $ind;
    private $tmpDir = __DIR__ . '/tmp/';

    public function setUp(): void {
        parent::setUp();
        $this->ind = new Indexer();
        $this->ind->setDepth(1);
        $this->ind->setParent(self::$res);
        $this->ind->setUploadSizeLimit(10000000);
        if (file_exists($this->tmpDir)) {
            system("rm -fR " . $this->tmpDir);
        }
    }

    public function tearDown(): void {
        parent::tearDown();
        if (file_exists($this->tmpDir)) {
            system("rm -fR " . $this->tmpDir);
        }
    }

    public function testSimple(): void {
        $this->ind->setFilter('/txt|xml/', Indexer::MATCH);
        $this->ind->setFilter('/^(skiptest.txt)$/', Indexer::SKIP);
        self::$repo->begin();
        $indRes = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(6, count($indRes));
    }

    public function testSkipNotExist(): void {
        $this->testSimple();

        $this->ind->setFilter('', Indexer::SKIP);
        $this->ind->setSkip(Indexer::SKIP_NOT_EXIST);
        self::$repo->begin();
        $indRes = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(6, count($indRes));
    }

    public function testSkipExist(): void {
        $indRes1 = $indRes2 = [];

        $this->ind->setFilter('/txt/', Indexer::MATCH);
        self::$repo->begin();
        $indRes1 = $this->ind->index();
        $this->noteResources($indRes1);
        self::$repo->commit();

        $this->assertEquals(4, count($indRes1));

        $this->ind->setSkip(Indexer::SKIP_EXIST);
        $this->ind->setFilter('/(txt|xml)$/', Indexer::MATCH);
        self::$repo->begin();
        $indRes2 = $this->ind->index();
        $this->noteResources($indRes2);
        self::$repo->commit();

        $this->assertEquals(2, count($indRes2));
    }

    public function testSkipBinaryExist(): void {
        $indRes1 = $indRes2 = [];

        $this->ind->setFilter('/txt/', Indexer::MATCH);
        self::$repo->begin();
        $indRes1 = $this->ind->index();
        $this->noteResources($indRes1);
        self::$repo->commit();

        $this->assertEquals(4, count($indRes1));

        $this->ind->setSkip(Indexer::SKIP_BINARY_EXIST);
        $this->ind->setFilter('/(txt|xml)$/', Indexer::MATCH);
        self::$repo->begin();
        $indRes2 = $this->ind->index();
        $this->noteResources($indRes2);
        self::$repo->commit();

        $this->assertEquals(4, count($indRes2));
    }

    public function testMetaFromFile(): void {
        $metaLookup = new MetaLookupFile(['.'], '.ttl');
        $this->ind->setDepth(0);
        $this->ind->setMetaLookup($metaLookup);
        $this->ind->setFilter('/sample.xml$/');
        self::$repo->begin();
        $indRes     = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    public function testMetaFromGraph(): void {
        $graph      = new Graph();
        $graph->parseFile(__DIR__ . '/data/sample.xml.ttl');
        $metaLookup = new MetaLookupGraph($graph, self::$repo->getSchema()->id);
        $this->ind->setDepth(0);
        $this->ind->setMetaLookup($metaLookup);
        $this->ind->setFilter('/sample.xml$/');
        self::$repo->begin();
        $indRes     = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    public function testSkipWithoutMetaInFile(): void {
        $metaLookup = new MetaLookupFile(['.'], '.ttl');
        self::$repo->begin();
        $this->ind->setMetaLookup($metaLookup, true);
        $this->ind->setFilter('/xml$/');
        $indRes     = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    public function testWithoutMetaInGraph(): void {
        $metaLookup = new MetaLookupFile(['.'], '.ttl');
        self::$repo->begin();
        $this->ind->setMetaLookup($metaLookup, true);
        $this->ind->setFilter('/sample.xml$/');
        $indRes     = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $meta = array_pop($indRes)->getMetadata();
        $this->assertEquals('sample value', (string) $meta->getLiteral('https://some.sample/property'));
    }

    public function testMergeOnExtMeta(): void {
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
        $this->ind->setPaths([basename($this->tmpDir)]);
        $this->ind->setFilter('/^' . $fileName . '$/');
        $this->ind->setMetaLookup(new MetaLookupFile(['.'], '.ttl'));
        $indRes = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes));
        $this->assertEquals($res1->getUri(), array_pop($indRes)->getUri());
    }

    public function testMergeAndDeleted(): void {
        $idProp    = self::$repo->getSchema()->id;
        $titleProp = self::$repo->getSchema()->label;
        $commonId = 'https://my.id.nmsp/' . rand();
        $fileName = rand();
        
        //first instance of a resource created in a separate transaction
        self::$repo->begin();
        $meta     = (new Graph())->resource('.');
        $meta->addResource($idProp, $commonId);
        $meta->addLiteral($titleProp, 'sample title');
        $res2     = self::$repo->createResource($meta);
        $this->noteResources([$res2]);
        self::$repo->commit();
        
        // main transaction
        self::$repo->begin();
        $res2->delete(true);
        $res3     = self::$repo->createResource($meta);
        $this->noteResources([$res3]);
        $res3->delete(true);
        $res4     = self::$repo->createResource($meta);
        $this->noteResources([$res4]);
        
        // preparare files on a disk
        $meta->delete($titleProp);
        mkdir($this->tmpDir);
        file_put_contents($this->tmpDir . $fileName . '.ttl', $meta->getGraph()->serialise('turtle'));
        file_put_contents($this->tmpDir . $fileName, 'sample content');
        // index
        $this->ind->setPaths(['tmp']);
        $this->ind->setFilter('/^' . $fileName . '$/');
        $this->ind->setMetaLookup(new MetaLookupFile(['.'], '.ttl'));
        $indRes    = $this->ind->index();
        $this->noteResources($indRes);
        self::$repo->commit();
        
        // indexed resource should match manually created one
        $this->assertEquals(1, count($indRes));
        $this->assertEquals($res4->getUri(), array_pop($indRes)->getUri());
    }

    /*
      public function testAutocommit(): void {
      $indRes = array();
      self::$repo->begin();
      $this->ind->setFilter('/txt|xml/');
      $this->ind->setAutoCommit(2);
      $indRes = $this->ind->index();
      assert(count($indRes) === 6, new Exception("resources count doesn't match " . count($indRes)));
      self::$repo->commit();
      }

      public function testNewVersionCreation(): void {
      $indRes1     = $indRes2     = $indRes3     = [];
      $origContent = file_get_contents(__DIR__ . '/data/sample.xml');
      $this->ind->setFilter('/^sample.xml$/', Indexer::MATCH);
      $this->ind->setFlatStructure(true);

      self::$repo->begin();
      $indRes1 = $this->ind->index();
      $initRes = $indRes1[array_keys($indRes1)[0]];
      $meta    = $initRes->getMetadata();
      $meta->addResource(RC::get('epicPidProp'), 'https://sample.pid');
      $initRes->setMetadata($meta);
      $initRes->updateMetadata();
      self::$repo->commit();

      file_put_contents(__DIR__ . '/data/sample.xml', random_int(0, 123456));

      self::$repo->begin();
      $this->ind->setVersioning(Indexer::VERSIONING_DIGEST, Indexer::PID_PASS);
      $indRes2 = $this->ind->index();
      self::$repo->commit();

      assert(count($indRes2) === 1, new Exception('Wrong indexed resources count'));
      $newRes      = $indRes2[array_keys($indRes2)[0]];
      $meta        = $newRes->getMetadata();
      assert((string) $meta->getResource(RC::get('epicPidProp')) === 'https://sample.pid', 'PID missing in the new resource');
      assert(in_array('https://sample.pid', $newRes->getIds()), 'PID missing among new resource IDs');
      $prevResUuid = (string) $meta->getResource(RC::get('fedoraIsNewVersionProp'));
      assert(!empty($prevResUuid), new Exception('No link to the previous version'));
      $prevRes     = self::$repo->getResourceById($prevResUuid);
      $prevMeta    = $prevRes->getMetadata(true);
      assert($prevMeta->getResource(RC::get('epicPidProp')) === null, 'PID present in the old resource');
      $newResUuid  = (string) $prevMeta->getResource(RC::get('fedoraIsPrevVersionProp'));
      assert(!empty($newResUuid), new Exception('No link to the newer version'));
      $newRes2     = self::$repo->getResourceById($newResUuid);
      assert($newRes2->getUri(true) === $newRes->getUri(true), new Exception('New version link points to a wrong resource'));

      file_put_contents(__DIR__ . '/data/sample.xml', random_int(0, 123456));

      self::$repo->begin();
      $this->ind->setVersioning(Indexer::VERSIONING_DIGEST, Indexer::PID_KEEP);
      $indRes3 = $this->ind->index();
      self::$repo->commit();

      assert(count($indRes3) === 1, new Exception('Wrong indexed resources count'));
      $newestRes  = $indRes3[array_keys($indRes3)[0]];
      $newestMeta = $newestRes->getMetadata();
      assert($newestMeta->getResource(RC::get('epicPidProp')) === null, 'PID present in the new resource');
      $newMeta    = $newRes->getMetadata(true);
      assert((string) $newMeta->getResource(RC::get('epicPidProp')) === 'https://sample.pid', 'PID not present in the old resource');
      assert(in_array('https://sample.pid', $newRes->getIds()), 'PID missing among old resource IDs');
      }
     */
}
