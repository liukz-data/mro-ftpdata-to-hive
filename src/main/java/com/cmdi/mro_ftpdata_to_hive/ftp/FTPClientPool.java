package com.cmdi.mro_ftpdata_to_hive.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 实现了一个FTPClient连接池
 *
 * @author heaven
 */
public class FTPClientPool implements ObjectPool,Serializable {
    private static final int DEFAULT_POOL_SIZE = 16;
   // private  LinkedBlockingQueue<FTPClientUtilTest> pool = new LinkedBlockingQueue<FTPClientUtilTest>();
    private  LinkedBlockingQueue<FTPClientUtil> pool;
    private final FTPClientFactory factory;
    private volatile boolean init = false;

    /**
     * @param factory
     * @throws Exception
     */
    public FTPClientPool(FTPClientFactory factory) throws Exception {
        this.factory = factory;
        pool = new LinkedBlockingQueue<FTPClientUtil>();
    }

    /**
     * 初始化连接池，需要注入一个工厂来提供FTPClient实例
     *
     * @param maxPoolSize
     * @throws Exception
     */
    public void initPool(int maxPoolSize) throws Exception {

            if(!init){
                synchronized (this){
                    if(!init) {
                        init = true;
                        for (int i = 0; i < maxPoolSize; i++) {
                            //往池中添加对象
                            addObject();
                        }
                    }
            }
        }


    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool.ObjectPool#borrowObject()
     */
    @Override
    public FTPClientUtil borrowObject() throws Exception {
        FTPClientUtil client = pool.take();
        if (client == null) {
            client = factory.makeObject(pool);
            addObject();
        } else if (!factory.validateObject(client)) {//验证不通过
            //使对象在池中失效
            invalidateObject(client);
            //制造并添加新对象到池中
            client = factory.makeObject(pool);
            addObject();
        }
        return client;

    }

    @Override
    public void returnObject(Object o) throws Exception {
        FTPClientUtil client = (FTPClientUtil) o;
        if ((client != null) && !pool.offer(client, 3, TimeUnit.SECONDS)) {

            factory.destroyObject(client);

        }
    }

    @Override
    public void invalidateObject(Object o) throws Exception {
        FTPClient client = (FTPClient) o;
        pool.remove(client);
    }


    /* (non-Javadoc)
     * @see org.apache.commons.pool.ObjectPool#addObject()
     */
    public void addObject() throws Exception {
        //插入对象到队列
        pool.offer(factory.makeObject(pool), 5, TimeUnit.SECONDS);
    }

    public int getNumIdle() throws UnsupportedOperationException {
        return 0;
    }

    public int getNumActive() throws UnsupportedOperationException {
        return this.pool.size();
    }
    public LinkedBlockingQueue<FTPClientUtil> getPool() {
        return this.pool;
    }

    public boolean isInit() {
        return init;
    }



    public void clear() {

    }

    /* (non-Javadoc)
     * @see org.apache.commons.pool.ObjectPool#close()
     */
    public void close() throws Exception {
        while (pool.iterator().hasNext()) {
            FTPClient client = pool.take();
            factory.destroyObject(client);
        }
    }

    public void setFactory(PoolableObjectFactory factory) throws IllegalStateException, UnsupportedOperationException {

    }

    @Override
    public String toString() {
        return "FTPClientPoolTest{" +
                "pool=" + pool +
                ", factory=" + factory +
                ", init=" + init +
                '}';
    }
}
