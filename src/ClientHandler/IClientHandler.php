<?php

declare(strict_types=1);

namespace Yurun\TDEngine\Orm\ClientHandler;

use Yurun\TDEngine\Exception\NetworkException;
use Yurun\TDEngine\Exception\OperationException;
use Yurun\TDEngine\Orm\Contract\IQueryResult;

interface IClientHandler
{
    /**
     * 查询
     * @param string $sql
     * @param string|null $name
     * @return IQueryResult
     * @throws  NetworkException|OperationException
     */
    public function query(string $sql, ?string $name = null): IQueryResult;
}
