<?php

declare(strict_types=1);

namespace Yurun\TDEngine\Orm\Test;

use PHPUnit\Framework\TestCase;
use Yurun\TDEngine\Orm\ClientHandler\Restful\Handler;
use Yurun\TDEngine\Orm\TDEngineOrm;
use Yurun\TDEngine\Orm\Test\Model\DeviceLogModel;
use Yurun\TDEngine\TDEngineManager;

class ModelTest extends TestCase
{
    protected function setUp(): void
    {
        TDEngineManager::setDefaultClientName('test');
        TDEngineOrm::setClientHandler(new Handler());
    }

    public function testCreateSuperTable(): void
    {
        DeviceLogModel::createSuperTable();
        $this->assertTrue(true);
    }

    public function testCreateTable(): void
    {
        $client = TDEngineManager::getClient('test');

        $table = 'device_' . md5(uniqid('', true));
        $deviceId = md5(uniqid('', true));
        DeviceLogModel::createTable($table, [$deviceId]);
        $this->assertTableExists($table);
        $client->sql('DROP TABLE device.' . $table);

        $table = 'device_' . md5(uniqid('', true));
        $deviceId = md5(uniqid('', true));
        DeviceLogModel::createTable($table, ['deviceId' => $deviceId]);
        $this->assertTableExists($table);
        $client->sql('DROP TABLE device.' . $table);

        $table = 'device_' . md5(uniqid('', true));
        $deviceId = md5(uniqid('', true));
        DeviceLogModel::createTable($table, ['device_id' => $deviceId]);
        $this->assertTableExists($table);
        $client->sql('DROP TABLE device.' . $table);
    }

    public function testInsert(): void
    {
        $table = 'device_insert';
        $record = new DeviceLogModel([], $table);
        $record->time = $time = (int) (microtime(true) * 1000);
        $record->deviceId = '00000001';
        $record->voltage = 1.23;
        $record->electricCurrent = 4.56;
        DeviceLogModel::batchInsert([$record]);

        $client = TDEngineManager::getClient('test');
        $result = $client->sql('select * from device.device_insert order by time desc limit 1');
        if ([
            [
                'time'             => $time,
                'voltage'          => 1.23,
                'electric_current' => 4.56,
            ],
        ] !== $result->getData() && [
            [
                'time'             => gmdate('Y-m-d\TH:i:s.', (int) ($time / 1000)) . substr((string) $time, -3, 3) . 'Z',
                'voltage'          => 1.23,
                'electric_current' => 4.56,
            ],
        ] !== $result->getData() && [
            [
                'time'             => gmdate('Y-m-d H:i:s.', (int) ($time / 1000)) . substr((string) $time, -3, 3),
                'voltage'          => 1.23,
                'electric_current' => 4.56,
            ],
        ] !== $result->getData())
        {
            var_dump($result->getData());
            $this->assertTrue(false);
        }
        $this->assertTrue(true);
    }

    public function testDeleteAll(): void
    {
        $time = (int) (microtime(true) * 1000);
        $condition = [
            ['<=', 'time', $time]
        ];
        DeviceLogModel::deleteAll($condition);

        $client = TDEngineManager::getClient('test');
        $result = $client->sql('select * from device.device_insert where time<=' . $time);
        if(!empty($result->getData())){
            $this->assertTrue(false);
        }

        $this->assertTrue(true);
    }

    public function testBatchInsert(): void
    {
        $table1 = 'device_batch_insert_1';
        $records = [];
        $record = new DeviceLogModel([], $table1);
        $record->time = $time1 = (int) (microtime(true) * 1000);
        $record->deviceId = '00000001';
        $record->voltage = 1.23;
        $record->electricCurrent = 4.56;
        $records[] = $record;

        usleep(1000);
        $table2 = 'device_batch_insert_2';
        $time = microtime(true);
        $time2 = (int) ($time * 1000);
        $data2 = [
            'time'            => gmdate('Y-m-d\TH:i:s.', (int) $time) . substr((string) $time2, -3, 3) . 'Z',
            'deviceId'        => '00000002',
            'voltage'         => 1.1,
            'electricCurrent' => 2.2,
        ];
        $records[] = new DeviceLogModel($data2, $table2);

        usleep(1000);
        $table3 = 'device_batch_insert_3';
        $time3 = (int) ($time * 1000);
        $data3 = [
            'time'            => gmdate('Y-m-d\TH:i:s.', (int) $time) . substr((string) $time3, -3, 3) . 'Z',
            'device_id'        => '00000003',
            'voltage'         => 1.3,
            'electric_current' => 5.2,
        ];
        $records[] = new DeviceLogModel($data3, $table3);
        DeviceLogModel::batchInsert($records);

        $client = TDEngineManager::getClient('test');
        $result = $client->sql('select * from device.device_batch_insert_1 order by time desc limit 1');
        if ([
            [
                'time'             => $time1,
                'voltage'          => 1.23,
                'electric_current' => 4.56,
            ],
        ] !== $result->getData() && [
            [
                'time'             => gmdate('Y-m-d\TH:i:s.', (int) ($time1 / 1000)) . substr((string) $time1, -3, 3) . 'Z',
                'voltage'          => 1.23,
                'electric_current' => 4.56,
            ],
        ] !== $result->getData() && [
            [
                'time'             => gmdate('Y-m-d H:i:s.', (int) ($time1 / 1000)) . substr((string) $time1, -3, 3),
                'voltage'          => 1.23,
                'electric_current' => 4.56,
            ],
        ] !== $result->getData())
        {
            var_dump($result->getData());
            $this->assertTrue(false);
        }

        $result = $client->sql('select * from device.device_batch_insert_2 order by time desc limit 1');
        if ([
            [
                'time'             => $time2,
                'voltage'          => 1.1,
                'electric_current' => 2.2,
            ],
        ] !== $result->getData() && [
            [
                'time'             => gmdate('Y-m-d\TH:i:s.', (int) ($time2 / 1000)) . substr((string) $time2, -3, 3) . 'Z',
                'voltage'          => 1.1,
                'electric_current' => 2.2,
            ],
        ] !== $result->getData() && [
            [
                'time'             => gmdate('Y-m-d H:i:s.', (int) ($time2 / 1000)) . substr((string) $time2, -3, 3),
                'voltage'          => 1.1,
                'electric_current' => 2.2,
            ],
        ] !== $result->getData())
        {
            var_dump($result->getData());
            $this->assertTrue(false);
        }

        $result = $client->sql('select * from device.device_batch_insert_3 order by time desc limit 1');
        if ([
                [
                    'time'             => $time3,
                    'voltage'         => 1.3,
                    'electric_current' => 5.2,
                ],
            ] !== $result->getData() && [
                [
                    'time'             => gmdate('Y-m-d\TH:i:s.', (int) ($time3 / 1000)) . substr((string) $time3, -3, 3) . 'Z',
                    'voltage'         => 1.3,
                    'electric_current' => 5.2,
                ],
            ] !== $result->getData() && [
                [
                    'time'             => gmdate('Y-m-d H:i:s.', (int) ($time3 / 1000)) . substr((string) $time3, -3, 3),
                    'voltage'         => 1.3,
                    'electric_current' => 5.2,
                ],
            ] !== $result->getData())
        {
            var_dump($result->getData());
            $this->assertTrue(false);
        }

        $this->assertTrue(true);
    }

    public function testQueryList(): void
    {
        $time = (int) (microtime(true) * 1000);
        $condition = [
            ['<=', 'time', $time]
        ];
        $colums = [
            'time as tttt',
            'voltage',
            'device_id',
        ];
        $pageSize = 2;
        $page = 1;
        $orderBy = 'voltage ASC';
        $result = DeviceLogModel::queryList($condition, $colums, $pageSize, $page, $orderBy);
        if(count($result->getData())!==2){
            var_dump($result->getData());
            $this->assertTrue(false);
        }
        $colums = [
            'device_id'
        ];
        $pageSize = 2;
        $page = 1;
        $orderBy = '';
        $groupBy = 'device_id';
        $result = DeviceLogModel::queryList($condition, $colums, $pageSize, $page, $orderBy, $groupBy);
        if(count($result->getData())!==3){
            var_dump($result->getData());
            $this->assertTrue(false);
        }
        $this->assertTrue(true);
    }

    private function assertTableExists(string $tableName): void
    {
        $result = TDEngineManager::getClient('test')->sql('show device.tables');
        foreach ($result->getData() as $row)
        {
            if ($tableName === $row['table_name'])
            {
                $this->assertTrue(true);

                return;
            }
        }
        $this->assertTrue(false);
    }
}
