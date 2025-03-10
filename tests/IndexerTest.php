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

use zozlak\RdfConstants as RDF;
use rdfInterface\QuadInterface;
use rdfInterface\DatasetInterface;
use rdfInterface\DatasetNodeInterface;
use quickRdf\DataFactory as DF;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use termTemplates\AnyOfTemplate;
use termTemplates\PredicateTemplate as PT;
use termTemplates\LiteralTemplate;
use quickRdfIo\Util as RdfIoUtil;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\ClientException;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\exception\Conflict;
use acdhOeaw\arche\lib\ingest\Indexer;
use acdhOeaw\arche\lib\ingest\IndexerException;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupFile;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupGraph;
use acdhOeaw\arche\lib\ingest\metaLookup\MetaLookupConstant;
use acdhOeaw\arche\lib\ingest\util\UUID;

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

//        MetaLookupFile::$debug  = 2;
//        MetaLookupGraph::$debug = 2;
//        Indexer::$debug         = 2;

        self::$repo->begin();
        $id = DF::namedNode('http://my.test/id');
        try {
            self::$res = self::$repo->getResourceById($id);
        } catch (NotFound $ex) {
            $meta      = new DatasetNode($id);
            $meta->add([
                DF::quadNoSubject(self::$schema->label, DF::literal('test parent')),
                DF::quadNoSubject(self::$schema->fileName, DF::literal('data')),
                DF::quadNoSubject(self::$schema->id, $id),
            ]);
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
            $file = (string) $i->getGraph()->getObject(new PT(self::$schema->fileName));
            if (is_file(__DIR__ . "/data/$file")) {
                $resp = self::$repo->sendRequest(new Request('get', (string) $i->getUri()));
                $this->assertEquals(file_get_contents(__DIR__ . "/data/$file"), (string) $resp->getBody());
            }
        }

        self::$repo->commit();
        foreach ($indRes as $i) {
            $file = (string) $i->getGraph()->getObject(new PT(self::$schema->fileName));
            if (is_file(__DIR__ . "/data/$file")) {
                $resp = self::$repo->sendRequest(new Request('get', (string) $i->getUri()));
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
        $tmpl = new PT('https://some.sample/property', new LiteralTemplate(null, LiteralTemplate::ANY));
        $this->assertEquals('sample value', (string) $meta->getObject($tmpl));
    }

    /**
     * @group indexer
     */
    public function testMetaFromGraph(): void {
        self::$test = 'testMetaFromGraph';

        $graph      = new Dataset();
        $graph->add(RdfIoUtil::parse(__DIR__ . '/data/sample.xml.ttl', new DF()));
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
        $tmpl = new PT('https://some.sample/property', new LiteralTemplate(null, LiteralTemplate::ANY));
        $this->assertEquals('sample value', (string) $meta->getObject($tmpl));
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
        $tmpl = new PT('https://some.sample/property', new LiteralTemplate(null, LiteralTemplate::ANY));
        $this->assertEquals('sample value', (string) $meta->getObject($tmpl));
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
        $tmpl = new PT('https://some.sample/property', new LiteralTemplate(null, LiteralTemplate::ANY));
        $this->assertEquals('sample value', (string) $meta->getObject($tmpl));
    }

    /**
     * @group indexer
     */
    public function testMergeOnExtMeta(): void {
        self::$test = 'testMergeOnExtMeta';

        $idProp    = self::$repo->getSchema()->id;
        $titleProp = self::$repo->getSchema()->label;
        $commonId  = DF::namedNode('https://my.id.nmsp/' . rand());
        $fileName  = rand();

        self::$repo->begin();

        $meta = new DatasetNode($commonId);
        $meta->add([
            DF::quadNoSubject($idProp, $commonId),
            DF::quadNoSubject($titleProp, DF::literal('sample title')),
        ]);
        $res1 = self::$repo->createResource($meta);
        $this->noteResources([$res1]);

        $meta->delete(new PT($titleProp));
        mkdir($this->tmpDir);
        RdfIoUtil::serialize($meta, 'text/turtle', $this->tmpDir . $fileName . '.ttl');
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
        $commonId  = DF::namedNode('https://my.id.nmsp/' . rand());
        $fileName  = rand();

        //first instance of a resource created in a separate transaction
        self::$repo->begin();
        $meta = new DatasetNode($commonId);
        $meta->add([
            DF::quadNoSubject($idProp, $commonId),
            DF::quadNoSubject($titleProp, DF::literal('sample title')),
        ]);
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
        $meta->delete(new PT($titleProp));
        mkdir($this->tmpDir);
        RdfIoUtil::serialize($meta, 'text/turtle', $this->tmpDir . $fileName . '.ttl');
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

        $id   = DF::namedNode('http://foo/' . rand());
        $meta = new DatasetNode($id);
        $meta->add(DF::quadNoSubject(self::$schema->id, $id));

        $this->ind->setFilter('/txt|xml/', Indexer::FILTER_MATCH);
        $this->ind->setFlatStructure(true);
        $this->ind->setMetaLookup(new MetaLookupConstant($meta));
        self::$repo->begin();
        $indRes = $this->ind->import(Indexer::ERRMODE_FAIL, 10, 20);
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

        $id   = DF::namedNode('http://foo/' . rand());
        $meta = new DatasetNode($id);
        $meta->add(DF::quadNoSubject(self::$schema->id, $id));

        $this->ind->setFilter('/txt|xml/', Indexer::FILTER_MATCH);
        $this->ind->setFlatStructure(true);
        $this->ind->setMetaLookup(new MetaLookupConstant($meta));
        self::$repo->begin();
        try {
            $indRes = $this->ind->import(Indexer::ERRMODE_FAIL, 10, 0);
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

        $verMetaFn = function (DatasetNodeInterface $oldMeta, Schema $schema): array {
            $repoIdNmsp = preg_replace('`/[0-9]+$`', '', (string) $oldMeta->getNode());
            $skipProp   = [$schema->id, $schema->pid];

            $newMeta = $oldMeta->copyExcept(new PT(new AnyOfTemplate($skipProp)));
            $newMeta->add(DF::quadNoSubject($schema->isNewVersionOf, $oldMeta->getNode()));
            $clbck   = function (QuadInterface $quad, DatasetInterface $ds) use ($newMeta,
                                                                                 $repoIdNmsp,
                                                                                 $schema) {
                $id = (string) $quad->getObject();
                if (!str_starts_with($id, $repoIdNmsp) && $ds->none($quad->withPredicate($schema->pid))) {
                    $newMeta->add($quad);
                    return null;
                }
                return $quad;
            };
            $oldMeta->forEach($clbck, new PT($schema->id));
            // so we don't end up with multiple versions of same filename in one collection
            $oldMeta->delete(new PT($schema->parent));
            // there is at least one non-internal id required; as all are being passed to the new resource, let's create a dummy one
            $oldMeta->add(DF::quadNoSubject($schema->id, DF::namedNode($schema->namespaces->vid . UUID::v4())));

            return [$oldMeta, $newMeta];
        };
        $verRefFn = function (RepoResource $old, RepoResource $new): void {
            $infoProp = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasVersionInfo');

            $meta = $old->getGraph();
            $meta->add(DF::quadNoSubject($infoProp, DF::literal("old resource info")));
            $old->setGraph($meta);
            $old->updateMetadata(RepoResource::UPDATE_MERGE, RepoResource::META_NONE);

            $meta = $new->getGraph();
            $meta->add(DF::quadNoSubject($infoProp, DF::literal("new resource info")));
            $new->setGraph($meta);
            $new->updateMetadata(RepoResource::UPDATE_MERGE, RepoResource::META_NONE);
        };

        $schema  = self::$repo->getSchema();
        $pidProp = $schema->ingest->pid;
        $pidTmpl = new PT($pidProp);
        $pid     = DF::namedNode('https://sample.pid/' . rand());

        $indRes1 = $indRes2 = $indRes3 = [];
        $this->ind->setFilter('/^sample.xml$/', Indexer::FILTER_MATCH);
        $this->ind->setFlatStructure(true);

        self::$repo->begin();
        $indRes1 = $this->ind->import();
        $this->noteResources($indRes1);
        $initRes = array_pop($indRes1);
        $meta    = $initRes->getMetadata();
        $meta->add(DF::quadNoSubject($pidProp, $pid));
        $initRes->setMetadata($meta);
        $initRes->updateMetadata();
        self::$repo->commit();

        file_put_contents(__DIR__ . '/data/sample.xml', random_int(0, 123456));

        self::$repo->begin();
        $this->ind->setVersioning(Indexer::VERSIONING_DIGEST, $verMetaFn, $verRefFn);
        $indRes2 = $this->ind->import();
        $this->noteResources($indRes2);
        self::$repo->commit();

        $this->assertEquals(1, count($indRes2));
        $newRes    = array_pop($indRes2);
        $meta      = $newRes->getMetadata();
        $this->assertEmpty($meta->getObject($pidTmpl)); // PID not copied to the new resource - depends on the repo recognizing pid property as a non-relation one
        $this->assertFalse(in_array($pid, $newRes->getIds()));
        $prevResId = (string) $meta->getObject(new PT($schema->isNewVersionOf));
        $this->assertTrue(!empty($prevResId));
        $prevRes   = self::$repo->getResourceById($prevResId);
        $prevMeta  = $prevRes->getMetadata();
        $this->assertEquals($pid, (string) $prevMeta->getObject($pidTmpl)); // PID present in the old resource
        $this->assertTrue(in_array($pid, $prevRes->getIds()));
        $infoTmpl  = new PT(DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasVersionInfo'));
        $this->assertEquals("old resource info", $prevMeta->getObjectValue($infoTmpl));
        $this->assertEquals("new resource info", $meta->getObjectValue($infoTmpl));
    }

    /**
     * @group indexer
     */
    public function testErrMode(): void {
        self::$test = 'testFailure';
        $prop       = self::$schema->label;
        self::$repo->begin();

        $meta = new DatasetNode(DF::namedNode('.'));
        $meta->add(DF::quadNoSubject($prop, DF::literal('foo', null, RDF::XSD_DATE)));
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
        $binSize = array_pop($indRes)->getMetadata()->getObject(new PT(self::$repo->getSchema()->binarySize));
        $this->assertEquals($count * $bufLen, (int) $binSize->getValue());
    }
}
