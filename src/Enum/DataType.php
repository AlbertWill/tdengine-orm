<?php

declare(strict_types=1);

namespace Yurun\TDEngine\Orm\Enum;

/**
 * 数据类型
 */
class DataType
{
    /**
     * 时间戳。缺省精度毫秒，可支持微秒和纳秒
     * Bytes:8
     */
    public const TIMESTAMP = 'TIMESTAMP';

    /**
     * 整型，范围 [-2^31, 2^31-1]
     * Bytes:4
     */
    public const INT = 'INT';

    /**
     * 无符号整数，[0, 2^32-1]
     * Bytes:4
     */
    public const INT_UNSIGNED = 'INT UNSIGNED';

    /**
     * 长整型，范围 [-2^63, 2^63-1]
     * Bytes:8
     */
    public const BIGINT = 'BIGINT';

    /**
     * 无符号的长整型，范围 [0, 2^64-1]
     * Bytes:8
     */
    public const BIGINT_UNSIGNED = 'BIGINT UNSIGNED';

    /**
     * 浮点型，有效位数 6-7，范围 [-3.4E38, 3.4E38]
     * Bytes:4
     */
    public const FLOAT = 'FLOAT';

    /**
     * 双精度浮点型，有效位数 15-16，范围 [-1.7E308, 1.7E308]
     * Bytes:8
     */
    public const DOUBLE = 'DOUBLE';

    /**
     * 记录单字节字符串，建议只用于处理 ASCII 可见字符，中文等多字节字符需使用 NCHAR
     * Bytes:自定义
     */
    public const BINARY = 'BINARY';

    /**
     * 短整型， 范围 [-32768, 32767]
     * Bytes:2
     */
    public const SMALLINT = 'SMALLINT';

    /**
     * 无符号短整型，范围 [0, 65535]
     * Bytes:2
     */
    public const SMALLINT_UNSIGNED = 'SMALLINT UNSIGNED';

    /**
     * 单字节整型，范围 [-128, 127]
     * Bytes:1
     */
    public const TINYINT = 'TINYINT';

    /**
     * 无符号单字节整型，范围 [0, 255]
     * Bytes:1
     */
    public const TINYINT_UNSIGNED = 'TINYINT UNSIGNED';

    /**
     * 布尔型
     * Bytes:1
     */
    public const BOOL = 'BOOL';

    /**
     * 记录包含多字节字符在内的字符串，如中文字符。每个 NCHAR 字符占用 4 字节的存储空间。
     * 字符串两端使用单引号引用，字符串内的单引号需用转义字符 \'。
     * NCHAR 使用时须指定字符串大小，类型为 NCHAR(10) 的列表示此列的字符串最多存储 10 个 NCHAR 字符。
     * 如果用户字符串长度超出声明长度，将会报错
     * Bytes:自定义
     */
    public const NCHAR = 'NCHAR';

    /**
     * JSON 数据类型， 只有 Tag 可以是 JSON 格式
     * Bytes:
     */
    public const JSON = 'JSON';

    /**
     * BINARY 类型的别名
     * Bytes:自定义
     */
    public const VARCHAR = 'VARCHAR';

    /**
     * 几何类型，3.1.0.0 版本开始支持
     * Bytes:自定义
     */
    public const GEOMETRY = 'GEOMETRY';

    /**
     * 可变长的二进制数据， 3.1.1.0 版本开始支持
     * Bytes:自定义
     */
    public const VARBINARY = 'VARBINARY';

    private function __construct()
    {
    }
}
