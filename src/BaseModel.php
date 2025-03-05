<?php

declare(strict_types=1);

namespace Yurun\TDEngine\Orm;

use Yurun\TDEngine\Orm\Annotation\Tag;
use Yurun\TDEngine\Orm\Contract\IQueryResult;
use Yurun\TDEngine\Orm\Enum\DataType;
use Yurun\TDEngine\Orm\Meta\Meta;
use Yurun\TDEngine\Orm\Meta\MetaManager;

/**
 * Model 基类.
 */
abstract class BaseModel implements \JsonSerializable
{
    /**
     * Meta.
     *
     * @var \Yurun\TDEngine\Orm\Meta\Meta
     */
    private $__meta;

    /**
     * 表名.
     *
     * @var string|null
     */
    protected $__table;

    /**
     * 构造方法
     * @param array $data
     * @param string|null $table
     */
    public function __construct(array $data = [], ?string $table = null)
    {
        $this->__meta = $meta = static::__getMeta();
        $this->__table = $table;
        //初始化字段
        if($data){
            foreach ($meta->getProperties() as $propertyName => $property)
            {
                if (isset($data[$propertyName]))
                {
                    $this->$propertyName = $data[$propertyName];
                }
                elseif (isset($data[$property->name]))
                {
                    $this->$propertyName = $data[$property->name];
                }
            }
        }

    }

    /**
     * 创建超级表
     * @param bool $ifNotExists
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function createSuperTable(bool $ifNotExists = true): IQueryResult
    {
        $meta = self::__getMeta();
        $tableAnnotation = $meta->getTable();
        $sql = 'CREATE TABLE ';
        if ($ifNotExists)
        {
            $sql .= 'IF NOT EXISTS ';
        }
        $fields = [];
        foreach ($meta->getFields() as $propertyName => $annotation)
        {
            $fields[] = '`' . ($annotation->name ?? $propertyName) . '` ' . $annotation->type . ($annotation->length > 0 ? ('(' . $annotation->length . ')') : '') . ($annotation->primary_key ? ' PRIMARY KEY' : '');
        }
        $sql .= self::getFullTableName() . ' (' . implode(',', $fields) . ')';

        $fields = [];
        foreach ($meta->getTags() as $propertyName => $annotation)
        {
            $fields[] = '`' . ($annotation->name ?? $propertyName) . '` ' . $annotation->type . ($annotation->length > 0 ? ('(' . $annotation->length . ')') : '');
        }
        $sql .= ' TAGS (' . implode(',', $fields) . ')';

        return TDEngineOrm::getClientHandler()->query($sql, $tableAnnotation->client ?? null);
    }

    /**
     * 创建表
     * @param string $tableName 表名
     * @param array $tags 标签数组
     * @param bool $ifNotExists
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function createTable(string $tableName, array $tags = [], bool $ifNotExists = true): IQueryResult
    {
        $meta = self::__getMeta();
        $tableAnnotation = $meta->getTable();
        if (!$tableAnnotation->super)
        {
            return self::createSuperTable($ifNotExists);
        }
        $sql = 'CREATE TABLE ';
        if ($ifNotExists)
        {
            $sql .= 'IF NOT EXISTS ';
        }
        $sql .=  '`' . $tableAnnotation->database . '`.`' . $tableName . '` USING ' . self::getFullTableName() . ' ';
        if ($tags)
        {
            if (array_is_list($tags))
            {
                $i = 0;
                $values = [];
                foreach ($meta->getTags() as $annotation)
                {
                    $values[] = self::parseValue($annotation->type, $tags[$i] ?? null);
                    ++$i;
                }
                if ($values)
                {
                    $sql .= 'TAGS (' . implode(',', $values) . ') ';
                }
            }
            else
            {
                $tagAnnotations = $meta->getTags();
                $propertiesByFieldName = $meta->getPropertiesByFieldName();
                $propertyNames = [];
                $values = [];
                foreach ($tags as $key => $value)
                {
                    if (isset($tagAnnotations[$key]))
                    {
                        $tagAnnotation = $tagAnnotations[$key];
                    }
                    elseif (isset($propertiesByFieldName[$key]) && $propertiesByFieldName[$key] instanceof Tag)
                    {
                        $tagAnnotation = $propertiesByFieldName[$key];
                    }
                    else
                    {
                        continue;
                    }
                    $propertyNames[] = '`' . ($tagAnnotation->name ?? $key) . '`';
                    $values[] = self::parseValue($tagAnnotation->type, $value);
                }
                if ($values)
                {
                    $sql .= '(' . implode(',', $propertyNames) . ') TAGS (' . implode(',', $values) . ') ';
                }
            }
        }

        return TDEngineOrm::getClientHandler()->query($sql, $tableAnnotation->client ?? null);
    }

    /**
     * 批量插入数据
     * @param array $models
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function batchInsert(array $models): IQueryResult
    {
        $sql = 'INSERT INTO ';
        foreach ($models as $model)
        {
            $meta = $model::__getMeta();
            $tableAnnotation = $meta->getTable();
            if ($tableAnnotation->super)
            {
                if (null === ($table = $model->__getTable()))
                {
                    throw new \RuntimeException('Table name cannot be null');
                }
                $sql .= '`' . $tableAnnotation->database . '`.`' . $table . '` USING ' . self::getFullTableName();
                $tags = $tagValues = [];
                foreach ($meta->getTags() as $propertyName => $tagAnnotation)
                {
                    $tags[] = '`' . ($tagAnnotation->name ?? $propertyName) . '`';
                    $tagValues[] = self::parseValue($tagAnnotation->type, $model->$propertyName);
                }
                if ($tags)
                {
                    $sql .= '(' . implode(',', $tags) . ') TAGS (' . implode(',', $tagValues) . ') ';
                }
            }
            $fields = $values = [];
            foreach ($meta->getFields() as $propertyName => $fieldAnnotation)
            {
                $fields[] = '`' . ($fieldAnnotation->name ?? $propertyName) . '`';
                $values[] = self::parseValue($fieldAnnotation->type, $model->$propertyName);
            }
            if ($fields)
            {
                $sql .= '(' . implode(',', $fields) . ') VALUES (' . implode(',', $values) . ') ';
            }
        }

        return TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);
    }

    /**
     * 根据条件删除数据
     * @param array $condition 条件数组
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function deleteAll(array $condition): IQueryResult
    {
        if(empty($condition)){
            throw new \RuntimeException('condition cannot be null');
        }
        $where = self::buildWhere(self::__getMeta(), $condition);
        $sql = 'DELETE FROM ' . self::getFullTableName() . ' ' .  $where;

        return TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);
    }

    /**
     * 根据条件查询数据
     * @param array $condition 条件数组
     * @param string|array $colums
     * @param int $pageSize
     * @param int $page
     * @param string|array $orderBy
     * @param string|array $groupBy
     * @return IQueryResult
     * @throws \Yurun\TDEngine\Exception\NetworkException
     * @throws \Yurun\TDEngine\Exception\OperationException
     */
    public static function queryList(array $condition, $colums, int $pageSize=0, int $page=1,  $orderBy='',  $groupBy=''): IQueryResult
    {
        if(empty($colums))
        {
            throw new \RuntimeException('colums cannot be null');
        }
        //查询字段
        $sql = 'SELECT ' . self::buildCommaSeparatedString($colums);
        //查询对象
        $sql .=  ' FROM ' . self::getFullTableName();
        //查询条件
        if($condition){
            $sql .= ' ' . self::buildWhere(self::__getMeta(), $condition);
        }
        //GROUP BY
        if($groupBy){
            $sql .= ' GROUP BY ' . self::buildCommaSeparatedString($groupBy);
        }
        //ORDER BY
        if($orderBy){
            $sql .= ' ORDER BY ' . self::buildCommaSeparatedString($orderBy);
        }
        //LIMIT
        if ($pageSize > 0 && $page > 0) {
            $offset = ($page - 1) * $pageSize;
            if($offset > 0){
                $sql .= ' LIMIT ' . $offset . ',' . $pageSize;
            }else{
                $sql .= ' LIMIT ' . $pageSize;
            }
        }

        return TDEngineOrm::getClientHandler()->query($sql, self::__getMeta()->getTable()->client ?? null);
    }

    /**
     * 获取完整的表名
     * @return string
     */
    public static function getFullTableName():string
    {
        $tableAnnotation = self::__getMeta()->getTable();
        return '`' . $tableAnnotation->database . '`.`' . $tableAnnotation->name . '`';
    }

    /**
     * 将输入（字符串或数组）转换为逗号分隔的字符串
     * @param string|array $input
     * @return string
     */
    public static function buildCommaSeparatedString($input): string
    {
        $str = '';
        if(is_string($input)){
            $str = $input;
        }else if (is_array($input)){
            $str = implode(',', $input);
        }
        return $str;
    }

    /**
     * 生成where条件
     * @param Meta $meta
     * @param array $condition
     * @return string
     */
    public static function buildWhere(Meta $meta, array $condition): string
    {
        $fieldTypeMap = self::getFieldTypeMap($meta);
        //生成where
        $where = [];
        foreach ($condition as $item){
            list($operator, $field, $value) = $item;
            //类型转换
            if(!isset($fieldTypeMap[$field])){
                continue;
            }
            $value_str = '';
            if(is_array($value)){
                $valueTmp = [];
                foreach ($value as $v){
                    $valueTmp[] = self::parseValue($fieldTypeMap[$field], $v);
                }
                $value_str =  '(' . implode(',', $valueTmp) . ')';
            }else{
                $value_str = self::parseValue($fieldTypeMap[$field], $value);
            }
            $where[] = '(`' . $field. '` ' .  $operator . ' ' . $value_str . ')';
        }
        $sql = 'WHERE ';
        if($where){
            $sql .= implode(' AND ', $where);
        }
        return $sql;
    }

    /**
     * 获取字段类型映射
     * @param Meta $meta
     * @return array
     */
    protected static function getFieldTypeMap(Meta $meta): array
    {
        //字段类型映射
        $fieldTypeMap = [];
        foreach ($meta->getFields() as $propertyName => $fieldAnnotation)
        {
            $field = $fieldAnnotation->name ?? $propertyName;
            $fieldTypeMap[$field] = $fieldAnnotation->type;
        }
        foreach ($meta->getTags() as $propertyName => $tagAnnotation)
        {
            $field = $tagAnnotation->name ?? $propertyName;
            $fieldTypeMap[$field] = $tagAnnotation->type;
        }
        return $fieldTypeMap;
    }

    /**
     * 获取模型元数据
     * @return Meta
     */
    public static function __getMeta(): Meta
    {
        return MetaManager::get(static::class);
    }

    /**
     * @param string $name
     * @return null
     */
    public function &__get(string $name)
    {
        $methodName = 'get' . ucfirst($name);
        if (method_exists($this, $methodName))
        {
            $result = $this->$methodName();
        }
        else
        {
            $result = null;
        }

        return $result;
    }

    /**
     * @param string $name
     * @param $value
     * @return void
     */
    public function __set(string $name, $value):void
    {
        $methodName = 'set' . ucfirst($name);

        $this->$methodName($value);
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    public function __isset($name)
    {
        return null !== $this->__get($name);
    }

    /**
     * @param string $name
     *
     * @return void
     */
    public function __unset($name)
    {
    }

    /**
     * 将当前对象作为数组返回.
     */
    public function toArray(): array
    {
        $result = [];
        foreach ($this->__meta->getProperties() as $propertyName => $_)
        {
            $result[$propertyName] = $this->__get($propertyName);
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    #[\ReturnTypeWillChange]
    public function jsonSerialize()
    {
        return $this->toArray();
    }

    public function __getTable(): ?string
    {
        return $this->__table;
    }

    public function __settable(?string $table): self
    {
        $this->__table = $table;

        return $this;
    }

    /**
     * @param string $type
     * @param $value
     * @return string
     */
    public static function parseValue(string $type, $value)
    {
        if (null === $value)
        {
            return 'NULL';
        }
        switch ($type)
        {
            case DataType::TIMESTAMP:
                if (!\is_string($value))
                {
                    break;
                }
                // no break
            case DataType::BINARY:
            case DataType::VARCHAR:
            case DataType::NCHAR:
                return '\'' . strtr($value, [
                    "\0"     => '\0',
                    "\n"     => '\n',
                    "\r"     => '\r',
                    "\t"     => '\t',
                    \chr(26) => '\Z',
                    \chr(8)  => '\b',
                    '"'      => '\"',
                    '\''     => '\\\'',
                    '_'      => '\_',
                    '%'      => '\%',
                    '\\'     => '\\\\',
                ]) . '\'';
            case DataType::BOOL:
                return $value ? 'true' : 'false';
        }

        return $value;
    }
}
