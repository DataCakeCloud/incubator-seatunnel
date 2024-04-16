/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.common.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.swing.text.html.parser.Entity;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

@Slf4j
public class Common {

    private Common() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Used to set the size when create a new collection(just to pass the checkstyle).
     */
    public static final int COLLECTION_SIZE = 16;

    private static final int APP_LIB_DIR_DEPTH = 2;

    private static final int PLUGIN_LIB_DIR_DEPTH = 3;

    private static DeployMode MODE;

    private static String SEATUNNEL_HOME;

    private static boolean STARTER = false;

    /**
     * Set mode. return false in case of failure
     */
    public static void setDeployMode(DeployMode mode) {
        MODE = mode;
    }

    public static void setStarter(boolean inStarter) {
        STARTER = inStarter;
    }

    public static DeployMode getDeployMode() {
        return MODE;
    }

    private static String getSeaTunnelHome() {

        if (StringUtils.isNotEmpty(SEATUNNEL_HOME)) {
            return SEATUNNEL_HOME;
        }
        String seatunnelHome = System.getProperty("SEATUNNEL_HOME");
        if (StringUtils.isBlank(seatunnelHome)) {
            seatunnelHome = System.getenv("SEATUNNEL_HOME");
        }
        if (StringUtils.isBlank(seatunnelHome)) {
            seatunnelHome = appRootDir().toString();
        }
        SEATUNNEL_HOME = seatunnelHome;
        return SEATUNNEL_HOME;
    }

    /**
     * Root dir varies between different spark master and deploy mode, it also varies between
     * relative and absolute path. When running seatunnel in --master local, you can put plugins
     * related files in $project_dir/plugins, then these files will be automatically copied to
     * $project_dir/seatunnel-core/target and token in effect if you start seatunnel in IDE tools
     * such as IDEA. When running seatunnel in --master yarn or --master mesos, you can put plugins
     * related files in plugins dir.
     */
    public static Path appRootDir() {
        log.info("SEATUNNEL_HOME===={}", SEATUNNEL_HOME);
        log.info("MODE===={}", MODE);
        log.info("STARTER===={}", STARTER);
        log.info("DeployMode.CLIENT===={}", DeployMode.CLIENT);
        log.info("DeployMode.RUN===={}", DeployMode.RUN);
        if (DeployMode.CLIENT == MODE || DeployMode.RUN == MODE || STARTER) {
            try {
                ProtectionDomain protectionDomain = Common.class.getProtectionDomain();
                log.info("protectionDomain===={}", protectionDomain);
                CodeSource codeSource = protectionDomain.getCodeSource();
                log.info("codeSource===={}", codeSource);
                URL location = codeSource.getLocation();
                log.info("location===={}", location);
                URI uri = location.toURI();
                log.info("uri===={}", uri);
                String path1 = uri.getPath();
                log.info("path1===={}", path1);
                String path =
                        Common.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI()
                                .getPath();
                if (StringUtils.isEmpty(path)) {
                    log.info("=====2===========path is empty");
                    String seatunnel_home = System.getenv("SEATUNNEL_HOME");
                    path = seatunnel_home + "/plugins/jdbc/lib/seatunnel-flink-13-starter.jar";
                    return Paths.get(path);
                }
                path = new File(path).getPath();
                log.info("path===={}", path);
                return Paths.get(path).getParent().getParent();
            } catch (URISyntaxException e) {
                log.info("RuntimeException===={}", e.toString());
                throw new RuntimeException(e);
            }
        } else if (DeployMode.CLUSTER == MODE || DeployMode.RUN_APPLICATION == MODE) {
            log.info("-------------------");
            return Paths.get("");
        } else {
            throw new IllegalStateException("deploy mode not support : " + MODE);
        }
    }

    public static Path appStarterDir() {
        Path path = appRootDir();
        log.info("appStarterDir===={}", path);
        return appRootDir().resolve("starter");
    }

    /**
     * Plugin Root Dir
     */
    public static Path pluginRootDir() {
        return Paths.get(getSeaTunnelHome(), "plugins");
    }

    /**
     * Plugin Connector Jar Dir
     */
    public static Path connectorJarDir(String engine) {
        return Paths.get(getSeaTunnelHome(), "connectors", engine.toLowerCase());
    }

    /**
     * Plugin Connector Dir
     */
    public static Path connectorDir() {
        return Paths.get(getSeaTunnelHome(), "connectors");
    }

    /**
     * lib Dir
     */
    public static Path libDir() {
        return Paths.get(getSeaTunnelHome(), "lib");
    }

    /**
     * return lib jars, which located in 'lib/*' or 'lib/{dir}/*'.
     */
    public static List<Path> getLibJars() {
        Path libRootDir = Common.libDir();
        if (!Files.exists(libRootDir) || !Files.isDirectory(libRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(libRootDir, APP_LIB_DIR_DEPTH, FOLLOW_LINKS)) {
            return stream.filter(it -> !it.toFile().isDirectory())
                    .filter(it -> it.getFileName().toString().endsWith(".jar"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * return the jar package configured in env jars
     */
    public static Set<Path> getThirdPartyJars(String paths) {

        return Arrays.stream(paths.split(";"))
                .filter(s -> !"".equals(s))
                .filter(it -> it.endsWith(".jar"))
                .map(path -> Paths.get(URI.create(path)))
                .collect(Collectors.toSet());
    }

    public static Path pluginTarball() {
        return appRootDir().resolve("plugins.tar.gz");
    }

    /**
     * return plugin's dependent jars, which located in 'plugins/${pluginName}/lib/*'.
     */
    public static List<Path> getPluginsJarDependencies() {
        Path pluginRootDir = Common.pluginRootDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(pluginRootDir, PLUGIN_LIB_DIR_DEPTH, FOLLOW_LINKS)) {
            return stream.filter(
                            it ->
                                    pluginRootDir.relativize(it).getNameCount()
                                            == PLUGIN_LIB_DIR_DEPTH)
                    .filter(it -> it.getParent().endsWith("lib"))
                    .filter(it -> it.getFileName().toString().endsWith(".jar"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
