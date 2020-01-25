package com.ethlo.keyvalue.compression;

public enum CompressionType
    {
        /**
         * Not recommended except when the underlying database does compression.
         * Use LZ4 if performance is most important.
         */
        NONE(0),

        /**
         * Recommended for very large scale databases or where I/O is costly.
         * Typical compression ratio of more than 12x.
         */
        LZMA2(3),

        /**
         * Recommended for large scale databases. Typical compression ratio of
         * 3x. Negligible CPU overhead.
         */
        LZ4(4),

        SNAPPY_FRAMED(5),

        SNAPPY_UNFRAMED_LEGACY(6);

        private final int id;

        CompressionType(int id)
        {
            this.id = id;
        }

        public static CompressionType valueOf(int type)
        {
            for (CompressionType c : CompressionType.values())
            {
                if (c.getId() == type)
                {
                    return c;
                }
            }
            throw new IllegalArgumentException("Unknown compression id " + type);
        }

        public int getId()
        {
            return this.id;
        }

        public int thresholdSize()
        {
            return 250;
        }
    }
