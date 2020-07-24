package de.metanome.algorithms.normalize;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.hpi.dataset_versioning.data.simplified.Attribute;
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable;
import de.hpi.dataset_versioning.io.IOService;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.BasicStatistic;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.normalize.config.Config;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.uni_potsdam.hpi.utils.FileUtils;
import de.metanome.algorithm_integration.results.basic_statistic_values.BasicStatisticValue;
import org.json.JSONObject;
import scala.Option;
import scala.collection.Set;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.HashSet;


public class Main {

	public static void main(String[] args) {
		Config conf = new Config();
		conf.inputFolderPath = args[0];
		conf.measurementsFolderPath = args[1];
		String tempResultDir = args[2];
		//conf.isHumanInTheLoop = true;
		//if (args.length != 0)
		//conf.setDataset(args[0]);
		System.out.println();
		/*try (Stream<Path> walk = Files.walk(Paths.get(conf.inputFolderPath))) {

			List<String> result = walk.filter(Files::isRegularFile)
					.sorted(Comparator.comparing(f -> Long.valueOf(f.toFile().length())))
					.map(x -> x.getFileName().toString().replace(".csv","")).collect(Collectors.toList());
			result.forEach(dataset-> {
				conf.inputDatasetName=dataset;
				executeNormi(conf);
			});*/
		try (Stream<Path> walk = Files.walk(Paths.get(conf.inputFolderPath))) {

			List<Path> result = walk
					.filter(Files::isRegularFile)
					.sorted(Comparator.comparing(f -> Long.valueOf(f.toFile().length())))
					.collect(Collectors.toList());

			result.forEach(dataset -> {
				conf.inputDatasetName = dataset.getFileName().toString().replace(".csv", "");
				conf.inputFolderPath=dataset.getParent()+File.separator;
				executeNormi(conf,tempResultDir);
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		//executeNormi(conf);
/**/		
	//	executeSchema();
    }
	
	private static void executeNormi(Config conf, String tempResultDir) {
		try {
			Normi normi = new Normi();
			normi.tempResultDir = tempResultDir;
			RelationalInputGenerator relationalInputGenerator = null;
			ResultCache resultReceiver = new ResultCache("MetanomeMock", null);
			
			relationalInputGenerator = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
					conf.inputFolderPath + conf.inputDatasetName + conf.inputFileEnding, true,
					conf.inputFileSeparator, conf.inputFileQuotechar, conf.inputFileEscape, conf.inputFileStrictQuotes, 
					conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader, conf.inputFileSkipDifferingLines, conf.inputFileNullString));
			
			normi.setRelationalInputConfigurationValue(Normi.Identifier.INPUT_GENERATOR.name(), relationalInputGenerator);
			normi.setResultReceiver(resultReceiver);
			
			// A human in the loop works only outside of Metanome. Hence, this is not a Metanome parameter
			normi.setIsHumanInTheLoop(conf.isHumanInTheLoop);
			String[] parts = conf.inputFolderPath.split(File.separator);
			normi.setsubFolder(parts[parts.length-1]);
			normi.execute();
			
			if (conf.writeResults) {
				//final String outputPath = conf.measurementsFolderPath + conf.inputDatasetName + File.separator;
				final String outputPath = conf.measurementsFolderPath;
				Stream<Result> results = resultReceiver.fetchNewResults().stream();
				
				final File resultFile = new File(outputPath + conf.resultFileName);
				FileUtils.createFile(outputPath + conf.resultFileName, false);
				
				final FileWriter writer = new FileWriter(resultFile, true);
				
				//results.map(result -> result.toString()).forEach(fd -> writeToFile(writer, fd));
				int decomposedTableID = 0;
				List<Result> collectedResults = results.collect(Collectors.toList());
				for (Result fd : collectedResults) {
					writeToFile(writer, conf.inputDatasetName,decomposedTableID, fd);
					decomposedTableID++;
				}
				System.out.println("Finished " + conf.inputDatasetName);
				writer.close();
				results.close();
			}
		}
		catch (AlgorithmExecutionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void executeClosureEvaluation(Config conf) {
		try {
			Normi normi = new Normi();
			
			RelationalInputGenerator relationalInputGenerator = null;
			ResultCache resultReceiver = new ResultCache("MetanomeMock", null);
			
			relationalInputGenerator = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
					conf.inputFolderPath + conf.inputDatasetName + conf.inputFileEnding, true,
					conf.inputFileSeparator, conf.inputFileQuotechar, conf.inputFileEscape, conf.inputFileStrictQuotes, 
					conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader, conf.inputFileSkipDifferingLines, conf.inputFileNullString));
			
			normi.setRelationalInputConfigurationValue(Normi.Identifier.INPUT_GENERATOR.name(), relationalInputGenerator);
			normi.setResultReceiver(resultReceiver);
			
			normi.evaluateCalculateClosure(30);
		}
		catch (AlgorithmExecutionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void writeToFile(FileWriter writer, String line) {
		try {
			writer.write(String.format("%s%n", line));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	private static void writeToFile(FileWriter writer, String DS, int decomposedTableID, Result line) {
//		try {
			BasicStatistic r= (BasicStatistic) line;
			//replace here if you need to change JSON format

			//method-1  basic_statistics as JSON
			//JSONObject jo = new JSONObject(r);

			//method-2 simplified JSON
			//because we use only FDs within the same tables i removed the table id from all fields but the id
			JSONObject jo = new JSONObject();
			//parse apart id and version:
			String originalID = DS.split("_")[1];
			LocalDate version = LocalDate.parse(DS.split("_")[0], IOService.dateTimeFormatter());
			ArrayBuffer<Attribute> schema = getSchemaList(DS, r);
			scala.collection.Set<String> pk = getPrimaryKey(DS,r);
			scala.collection.Set<String> fks = getForeignKey(DS,r);
			DecomposedTable res = new DecomposedTable(originalID,version,decomposedTableID, schema,pk,fks);
			res.appendToWriter(writer,false,true,true);
//			jo.put("Table_id", DS);
//			jo.put("Schema", r.getColumnCombination().toString().replace(".csv","").replace(DS+".",""));
//			for (Map.Entry<String, BasicStatisticValue> entry : r.getStatisticMap().entrySet()) {
//				jo.put(entry.getKey().toString(), entry.getValue().toString().replace(".csv","").replace(DS+".",""));
//			}
//			writer.write(jo.toString()+System.lineSeparator());
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
	}

	private static Set<String> getForeignKey(String DS, BasicStatistic r) {
		HashSet<String> fk = new HashSet<>();
		for (Map.Entry<String, BasicStatisticValue> entry : r.getStatisticMap().entrySet()) {
			if(entry.getKey()=="ForeignKey"){
				String fkStr = entry.getValue().toString().replace(".csv","").replace(DS+".","");
				Arrays.asList(fkStr.split(",")).stream().map(s -> s.trim()).forEach(s -> fk.add(s));
			}
		}
		return fk;
	}

	private static Set<String> getPrimaryKey(String DS, BasicStatistic r) {
		HashSet<String> pk = new HashSet<>();
		for (Map.Entry<String, BasicStatisticValue> entry : r.getStatisticMap().entrySet()) {
			if(entry.getKey()=="PrimaryKey"){
				String pkStr = entry.getValue().toString().replace(".csv","").replace(DS+".","");
				Arrays.asList(pkStr.split(",")).stream().map(s -> s.trim()).forEach(s -> pk.add(s));
			}
		}
		return pk;
	}

	private static ArrayBuffer<Attribute> getSchemaList(String DS, BasicStatistic r) {
		ArrayBuffer<Attribute> schemaAsScala = new ArrayBuffer<Attribute>();
		String schemaString = r.getColumnCombination().toString().replace(".csv","").replace(DS+".","");
		schemaString = schemaString.substring(1,schemaString.length()-1);
		List<String> schema = Arrays.asList(schemaString.split(","));
		schema.forEach(s -> schemaAsScala.addOne(new Attribute(s,-1, Option.empty(),Option.empty())));
		return schemaAsScala;
	}
}
