package com.ververica;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class StreamingJob {

    private static final Pattern USE_DIALECT = Pattern.compile("USE DIALECT (.+);?", Pattern.CASE_INSENSITIVE);

    private static final Pattern USE_CATALOG = Pattern.compile("USE CATALOG (.+);?", Pattern.CASE_INSENSITIVE);

    private static final String HIVE_CATALOG_NAME = "hive_catalog";

    public static void main(String[] args) {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String file = tool.get("sql");

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        if (tool.has("hive-conf")) {
            String hiveConf = tool.get("hive-conf");
            String hiveVersion = tool.get("hive-version", "1.1.0");
            System.out.println("Registering hive catalog: version " + hiveVersion);
            Catalog hive = new HiveCatalog(HIVE_CATALOG_NAME, null, hiveConf, hiveVersion);
            env.registerCatalog(HIVE_CATALOG_NAME, hive);
        }

        String contents = readFile(file);
        String[] statements = Arrays.stream(contents.split(";"))
                .map(String::trim)
                .filter(statement -> !statement.isEmpty())
                .toArray(String[]::new);

        for (String statement : statements) {
            System.out.println(statement);

            if (useDialect(env, statement)) {
                continue;
            }

            if (useCatalog(env, statement)) {
                continue;
            }

            try {
                env.executeSql(statement);
            } catch (SqlParserException exception) {
                throw new SqlParserException("Failed to parse statement:\n" + statement + "\n", exception);
            }
        }
    }

    private static boolean useDialect(TableEnvironment env, String statement) {
        Matcher matcher = USE_DIALECT.matcher(statement);
        if (matcher.matches()) {
            String dialect = matcher.group(1);
            if (dialect.equalsIgnoreCase("hive")) {
                env.getConfig().setSqlDialect(SqlDialect.HIVE);
            } else if (dialect.equalsIgnoreCase("default")) {
                env.getConfig().setSqlDialect(SqlDialect.DEFAULT);
            } else {
                throw new IllegalStateException("Unsupported Dialect " + dialect);
            }
            return true;
        }

        return false;
    }

    private static boolean useCatalog(TableEnvironment env, String statement) {
        Matcher matcher = USE_CATALOG.matcher(statement);
        if (matcher.matches()) {
            String catalog = matcher.group(1);
            env.useCatalog(catalog);
            return true;
        }

        return false;
    }

    private static String readFile(String filePath) {
        StringBuilder contentBuilder = new StringBuilder();

        try (Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read sql file", e);
        }

        return contentBuilder.toString();
    }
}
