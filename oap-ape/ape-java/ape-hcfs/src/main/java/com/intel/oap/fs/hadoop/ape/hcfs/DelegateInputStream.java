package com.intel.oap.fs.hadoop.ape.hcfs;

import com.intel.oap.fs.hadoop.ape.hcfs.redis.RedisUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;

public class DelegateInputStream extends FSInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(DelegateInputStream.class);

    private final FSDataInputStream hdfsInputStream;
    private final Configuration conf;
    private byte[] oneByte;

    private boolean enableReadMetrics = true;
    private long readCount = 0;
    private long readLength = 0;
    private long readTime = 0;
    private long computeWaitTime = 0;
    private long lastReadTime = 0;
    private long streamCount = 0;
    private long streamOpenTime = 0;
    private long streamLifeTime = 0;

    public DelegateInputStream(FSDataInputStream hdfsInputStream, Configuration conf) {
        this.hdfsInputStream = hdfsInputStream;
        this.conf = conf;

        streamCount += 1;
        streamOpenTime = System.nanoTime();
        if (enableReadMetrics) {
            Jedis jedis = null;
            try {
                jedis = RedisUtils.getRedisClient(conf).getJedis();
                jedis.hsetnx("rm_set", "rm_streamStart", String.valueOf(streamOpenTime));
            } catch (Exception e) {
                throw new JedisException(e.getMessage(), e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        LOG.info("DelegateInputStream init");
    }

    @Override
    public void seek(long l) throws IOException {
        hdfsInputStream.seek(l);
    }

    @Override
    public long getPos() throws IOException {
        return hdfsInputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return hdfsInputStream.seekToNewSource(l);
    }

    @Override
    public int read() throws IOException {
        if (oneByte == null) {
            oneByte = new byte[1];
        }
        if (read(oneByte, 0, 1) <= 0) {
            return -1;
        }
        return oneByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long startTime = System.nanoTime();

        int ret = hdfsInputStream.read(b, off, len);

        long endTime = System.nanoTime();
        readCount += 1;
        readLength += ret;
        readTime += (endTime - startTime);
        if (lastReadTime != 0) {
            computeWaitTime += (startTime - lastReadTime);
        }
        lastReadTime = endTime;

        return ret;
    }

    @Override
    public int available() throws IOException {
        return hdfsInputStream.available();
    }

    @Override
    public long skip(long n) throws IOException {
        return hdfsInputStream.skip(n);
    }

    @Override
    public void close() throws IOException {
        LOG.info("DelegateInputStream close");

        long streamEnd = System.nanoTime();
        streamLifeTime = streamEnd - streamOpenTime;

        if (enableReadMetrics) {
            Jedis jedis = null;
            try {
                jedis = RedisUtils.getRedisClient(conf).getJedis();
                jedis.hincrBy("rm_set", "rm_readCount", readCount);
                jedis.hincrBy("rm_set", "rm_readLength", readLength);
                jedis.hincrBy("rm_set", "rm_readTime", readTime);
                jedis.hincrBy("rm_set", "rm_computeWaitTime", computeWaitTime);
                jedis.hincrBy("rm_set", "rm_streamLifeTime", streamLifeTime);
                jedis.hincrBy("rm_set", "rm_streamCount", streamCount);
                jedis.hset("rm_set", "rm_streamEnd", String.valueOf(streamEnd));

                LOG.info("reading metrics saved to redis");
            } catch (Exception e) {
                throw new JedisException(e.getMessage(), e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        hdfsInputStream.close();
        super.close();
    }
}
