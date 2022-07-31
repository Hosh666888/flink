import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.delegation.BlinkPlannerFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.delegation.StreamExecutor;

import java.util.HashMap;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 2022/7/31 11:48
 * @desc:
 **/
public class CreateTableDemo {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        CatalogManager catalogManager = new CatalogManager("myCatalog", new GenericInMemoryCatalog("my", "myDatabase"));
        ModuleManager moduleManager = new ModuleManager();

        TableConfig tableConfig = new TableConfig();
        FunctionCatalog functionCatalog = new FunctionCatalog(
                tableConfig,catalogManager,moduleManager
        );

        StreamExecutor streamExecutor = new StreamExecutor(env);


        Planner planner = new BlinkPlannerFactory().create(new HashMap<String, String>(), streamExecutor, tableConfig, functionCatalog, catalogManager);
        StreamTableEnvironmentImpl tableEnv = new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                functionCatalog,
                tableConfig,
                env,
                planner,
                streamExecutor,
                true
        );

        Table from = tableEnv.from("create table(name String,age BIGINT)");



    }
}
