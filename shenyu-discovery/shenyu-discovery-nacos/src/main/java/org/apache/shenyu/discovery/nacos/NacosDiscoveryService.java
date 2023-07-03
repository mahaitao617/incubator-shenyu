/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.discovery.nacos;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.AbstractEventListener;
import com.alibaba.nacos.api.naming.listener.Event;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.common.exception.ShenyuException;
import org.apache.shenyu.discovery.api.ShenyuDiscoveryService;
import org.apache.shenyu.discovery.api.config.DiscoveryConfig;
import org.apache.shenyu.discovery.api.listener.DataChangedEventListener;
import org.apache.shenyu.spi.Join;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Nacos for shenyu discovery service.
 */
@Join
public class NacosDiscoveryService implements ShenyuDiscoveryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NacosDiscoveryService.class);

    private final Map<String, String> nodeDataMap = new HashMap<>();

    private static final String NAMESPACE = "nacosNameSpace";

    private ConfigService configService;

    private NamingService namingService;

    @Override
    public void init(final DiscoveryConfig config) {
        String serverAddr = config.getServerList();
        Properties properties = config.getProps();
        Properties nacosProperties = new Properties();
        nacosProperties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);

        nacosProperties.put(PropertyKeyConst.NAMESPACE, properties.getProperty(NAMESPACE));
        // the nacos authentication username
        nacosProperties.put(PropertyKeyConst.USERNAME, properties.getProperty(PropertyKeyConst.USERNAME, ""));
        // the nacos authentication password
        nacosProperties.put(PropertyKeyConst.PASSWORD, properties.getProperty(PropertyKeyConst.PASSWORD, ""));
        // access key for namespace
        nacosProperties.put(PropertyKeyConst.ACCESS_KEY, properties.getProperty(PropertyKeyConst.ACCESS_KEY, ""));
        // secret key for namespace
        nacosProperties.put(PropertyKeyConst.SECRET_KEY, properties.getProperty(PropertyKeyConst.SECRET_KEY, ""));
        try {
            this.configService = ConfigFactory.createConfigService(nacosProperties);
            this.namingService = NamingFactory.createNamingService(nacosProperties);
        } catch (NacosException e) {
            throw new ShenyuException(e);
        }
    }

    @Override
    public void watcher(final String key, final DataChangedEventListener listener) {
        if (StringUtils.isEmpty(key)) {
            return;
        }
        try {
            namingService.subscribe(key,new AbstractEventListener() {
                @Override
                public void onEvent(Event event) {

                }
            });
        }catch (NacosException e) {
            throw new ShenyuException(e);
        }
    }

    @Override
    public void register(final String key, final String value) {
    }

    @Override
    public String getData(final String key) {
        return "";
    }
}
