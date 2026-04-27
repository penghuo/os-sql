/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni.exchange;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.transport.AuxTransport;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.opensearch.plugin.omni.OmniSettings;

/**
 * AuxTransport that serves Trino page exchange protocol over HTTP.
 * Binds to a dedicated port (default 9500) with 3 routes:
 * - GET /v1/task/{taskId}/results/{bufferId}/{token} — fetch pages
 * - GET /v1/task/{taskId}/results/{bufferId}/{token}/acknowledge — acknowledge pages
 * - DELETE /v1/task/{taskId}/results/{bufferId} — destroy buffer
 */
public class Netty4ExchangeTransport extends AuxTransport {

    private static final Logger log = LogManager.getLogger(Netty4ExchangeTransport.class);
    public static final String SETTING_KEY = "omni-exchange";

    private final Settings settings;
    private final NetworkService networkService;
    private final ExchangeHttpHandler handler;

    private volatile EventLoopGroup bossGroup;
    private volatile EventLoopGroup workerGroup;
    private volatile Channel serverChannel;
    private volatile BoundTransportAddress boundAddress;

    public Netty4ExchangeTransport(
            Settings settings,
            NetworkService networkService,
            ExchangeHttpHandler handler) {
        this.settings = settings;
        this.networkService = networkService;
        this.handler = handler;
    }

    @Override
    public String settingKey() {
        return SETTING_KEY;
    }

    @Override
    public BoundTransportAddress getBoundAddress() {
        return boundAddress;
    }

    @Override
    protected void doStart() {
        int port = OmniSettings.EXCHANGE_PORT.get(settings);

        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("omni-exchange-boss"));
        int workerThreads = Runtime.getRuntime().availableProcessors();
        workerGroup = new NioEventLoopGroup(workerThreads, new DefaultThreadFactory("omni-exchange-worker"));
        log.info("Exchange transport using {} worker threads", workerThreads);

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(32 * 1024 * 1024));
                        ch.pipeline().addLast(handler);
                    }
                })
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        try {
            InetAddress bindAddress = InetAddress.getByName("0.0.0.0");
            serverChannel = bootstrap.bind(bindAddress, port).sync().channel();
            InetSocketAddress localAddr = (InetSocketAddress) serverChannel.localAddress();
            TransportAddress publishAddress = new TransportAddress(localAddr);
            boundAddress = new BoundTransportAddress(
                    new TransportAddress[]{publishAddress}, publishAddress);
            log.info("Exchange transport bound to [{}:{}]", localAddr.getAddress().getHostAddress(), localAddr.getPort());
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof BindException) {
                throw new RuntimeException("Exchange transport port " + port + " already in use", cause);
            }
            throw new RuntimeException("Failed to bind exchange transport on port " + port, e);
        }
    }

    @Override
    protected void doStop() {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void doClose() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }
    }
}
