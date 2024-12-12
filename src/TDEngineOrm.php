<?php

declare(strict_types=1);

namespace Yurun\TDEngine\Orm;

use Yurun\TDEngine\Orm\ClientHandler\IClientHandler;

class TDEngineOrm
{
    /**
     * @var IClientHandler|null
     */
    private static $clientHandler;

    private function __construct()
    {
    }

    /**
     * 设置客户端
     * @param IClientHandler $clientHandler
     */
    public static function setClientHandler(IClientHandler $clientHandler): void
    {
        self::$clientHandler = $clientHandler;
    }

    /**
     * 获取客户端
     * @return IClientHandler
     */
    public static function getClientHandler(): IClientHandler
    {
        if (self::$clientHandler)
        {
            return self::$clientHandler;
        }
        else
        {
            if (class_exists(\TDengine\Connection::class, false))
            {
                return self::$clientHandler = new \Yurun\TDEngine\Orm\ClientHandler\Extension\Handler();
            }
            else
            {
                return self::$clientHandler = new \Yurun\TDEngine\Orm\ClientHandler\Restful\Handler();
            }
        }
    }
}
