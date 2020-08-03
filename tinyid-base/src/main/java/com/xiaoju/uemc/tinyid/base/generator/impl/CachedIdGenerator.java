package com.xiaoju.uemc.tinyid.base.generator.impl;

import com.xiaoju.uemc.tinyid.base.entity.Result;
import com.xiaoju.uemc.tinyid.base.entity.ResultCode;
import com.xiaoju.uemc.tinyid.base.entity.SegmentId;
import com.xiaoju.uemc.tinyid.base.exception.TinyIdSysException;
import com.xiaoju.uemc.tinyid.base.generator.IdGenerator;
import com.xiaoju.uemc.tinyid.base.service.SegmentIdService;
import com.xiaoju.uemc.tinyid.base.util.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author du_imba
 */
public class CachedIdGenerator implements IdGenerator {
    protected String bizType;
    protected SegmentIdService segmentIdService;
    // 当前ID段
    protected volatile SegmentId current;
    // 下一次ID段
    protected volatile SegmentId next;
    // 是否正在加载下一个ID段
    private volatile boolean isLoadingNext;
    private Object lock = new Object();
    private ExecutorService executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("tinyid-generator"));

    public CachedIdGenerator(String bizType, SegmentIdService segmentIdService) {
        this.bizType = bizType;
        this.segmentIdService = segmentIdService;
        loadCurrent();
    }

    public synchronized void loadCurrent() {
        // 如果当前ID段不可用了
        if (current == null || !current.useful()) {
            // 判断下一个ID段是否可用
            if (next == null) {
                // 下一个ID段也不可用， 则重新获取一次ID段
                SegmentId segmentId = querySegmentId();
                this.current = segmentId;
            } else {
                // 如果下一个ID段可用，则将下一个ID段提升为当前ID段
                current = next;
                next = null;
            }
        }
    }

    // 获取新的ID段
    private SegmentId querySegmentId() {
        String message = null;
        try {
            SegmentId segmentId = segmentIdService.getNextSegmentId(bizType);
            if (segmentId != null) {
                return segmentId;
            }
        } catch (Exception e) {
            message = e.getMessage();
        }
        throw new TinyIdSysException("error query segmentId: " + message);
    }

    public void loadNext() {
        if (next == null && !isLoadingNext) {
            synchronized (lock) {
                if (next == null && !isLoadingNext) {
                    isLoadingNext = true;
                    // 在一个异步线程中，加载下一个ID段
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                // 无论获取下个segmentId成功与否，都要将isLoadingNext赋值为false
                                next = querySegmentId();
                            } finally {
                                isLoadingNext = false;
                            }
                        }
                    });
                }
            }
        }
    }

    @Override
    public Long nextId() {
        while (true) {
            // 如果当前ID段不可用，则不断重试， 确保当前ID段可用
            if (current == null) {
                loadCurrent();
                continue;
            }
            // 生成下一个ID
            Result result = current.nextId();

            if (result.getCode() == ResultCode.OVER) {
                // 超过了最大值，需要重新校准，
                loadCurrent();
            } else {
                // 已经使用超过了指定的阈值，触发 加载下一批次号
                if (result.getCode() == ResultCode.LOADING) {
                    loadNext();
                }
                // 只是触发了 加载下一批次号的操作， 当前返回的Id 还是能用的
                return result.getId();
            }
        }
    }

    /**
     *
     * 一次获取一批的ID段
     *
     * @param batchSize
     * @return
     */
    @Override
    public List<Long> nextId(Integer batchSize) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Long id = nextId();
            ids.add(id);
        }
        return ids;
    }

}
