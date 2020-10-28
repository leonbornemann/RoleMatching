package de.metanome.algorithms.normalize;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema;
import de.hpi.dataset_versioning.data.simplified.Attribute;
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd.FDValidator;
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTable;
import de.hpi.dataset_versioning.io.DBSynthesis_IOService;
import de.hpi.dataset_versioning.io.IOService;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.normalize.config.Config;
import de.metanome.algorithms.normalize.structures.Schema;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import org.json.JSONObject;
import scala.collection.Set;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.HashSet;


public class Main {

	private static String subdomain;
	private static Config conf;
	private static String datasetID;
	private static boolean runStatistics = false;
	private static boolean runOnlyStatistics = false;
	private static int MAX_FD_SIZE_FOR_UNION = 4;
	private static int maxFDLHSSize = -1;
	private static boolean limitLHSSizeBetweenIntersectionSteps;

	public static void main(String[] args) {
		IOService.socrataDir_$eq(args[0]);
		subdomain = args[1];
		datasetID = args[2];
		runOnlyStatistics = Boolean.parseBoolean(args[3]);
		maxFDLHSSize = Integer.parseInt(args[4]);
		limitLHSSizeBetweenIntersectionSteps = Boolean.parseBoolean(args[5]);
		conf = new Config();
		///home/leon/data/dataset_versioning/socrata/fromServer/db_synthesis/decomposition/csv/org.cityofchicago/
		conf.inputFolderPath = DBSynthesis_IOService.getExportedCSVSubdomainDir(subdomain).getAbsolutePath() + File.separator;
		///home/leon/data/dataset_versioning/socrata/fromServer/db_synthesis/decomposition/measurements/
		conf.measurementsFolderPath = DBSynthesis_IOService.getDecomposedTablesDir(subdomain).getAbsolutePath() + File.separator;
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

		try (Stream<Path> walk = Files.walk(Paths.get(conf.inputFolderPath + File.separator + datasetID))) {

			executeNormalizationForID(walk);
		} catch (IOException | AlgorithmExecutionException e) {
			e.printStackTrace();
		}
    }

	private static void executeNormalizationForID(Stream<Path> walk) throws AlgorithmExecutionException, IOException {
		List<Path> result = walk
				.filter(Files::isRegularFile)
				.sorted(Comparator.comparing(f -> LocalDate.parse(f.getFileName().toString().split("\\.")[0], IOService.dateTimeFormatter()).toEpochDay()))
				.collect(Collectors.toList());
		FDValidator validator = new FDValidator(subdomain, datasetID,MAX_FD_SIZE_FOR_UNION);
		Map<BitSet, BitSet> intersectedFds = validator.getFDIntersection(limitLHSSizeBetweenIntersectionSteps);
		//additional filter step to make normalization possible if the FD size is smaller:
		Map<BitSet, BitSet> filteredBySize = new HashMap<BitSet, BitSet>();
		for (BitSet lhs : intersectedFds.keySet()) {
			BitSet rhs = intersectedFds.get(lhs);
			if(lhs.cardinality()<=maxFDLHSSize || maxFDLHSSize ==-1){
				filteredBySize.put(lhs,rhs);
			}
		}
		intersectedFds = filteredBySize;
		//done with additional filtering
		Path lastDataset = result.get(result.size() - 1);
		LocalDate dateOfLast = LocalDate.parse(lastDataset.getFileName().toString().split("\\.")[0], IOService.dateTimeFormatter());
		Path datasetVersionCSV = DBSynthesis_IOService.getExportedCSVFile(subdomain, datasetID, dateOfLast).toPath();
		Path datasetVersionFD = DBSynthesis_IOService.getFDFile(subdomain, datasetID, dateOfLast).toPath();
		//execute for the last one:
		conf.inputDatasetName = datasetVersionCSV.getFileName().toString().replace(".csv", "");
		conf.inputFolderPath = datasetVersionCSV.getParent() + File.separator;
		Normi normi = new Normi();
		ResultCache resultReceiver = configureNormi(conf, datasetVersionFD.getParent().getParent().toString(), normi);
		normi.setResultReceiver(resultReceiver);
		List<Schema> bcnf = normi.runNormalization(intersectedFds,true);
		Stream<Result> results = resultReceiver.fetchNewResults().stream();
		List<Result> collectedResults = results.collect(Collectors.toList());
		if (conf.writeResults && !runOnlyStatistics) {
			//final String outputPath = conf.measurementsFolderPath + conf.inputDatasetName + File.separator;
			final File resultFile = DBSynthesis_IOService.getDecomposedTableFile(subdomain,datasetID,dateOfLast);
			final FileWriter writer = new FileWriter(resultFile, false);
			//results.map(result -> result.toString()).forEach(fd -> writeToFile(writer, fd));
			int decomposedTableID = 0;
			for (Schema decomposedTable : bcnf) {
				writeToFile(writer,normi,datasetID, dateOfLast, decomposedTableID, decomposedTable);
				decomposedTableID++;
			}
			System.out.println("Finished " + conf.inputDatasetName);
			writer.close();
			results.close();
		}
		if(runStatistics) {
			File statDir = DBSynthesis_IOService.getStatisticsDir(subdomain,datasetID);
			Map<BitSet, BitSet> unfilteredFdsForLast = getFdsForFile(datasetID, datasetVersionCSV, datasetVersionFD);
			PrintWriter statWriter = new PrintWriter(statDir.getAbsolutePath() + "_fd_statistics.csv");
			Normi normi2 = new Normi();
			ResultCache resultReceiver2 = configureNormi(conf, datasetVersionFD.getParent().getParent().toString(), normi2);
			normi2.setResultReceiver(resultReceiver2);
			List<Schema> bcnf2 = normi2.runNormalization(unfilteredFdsForLast,false);
			List<Result> resultsWithOriginalFD = resultReceiver2.fetchNewResults();
			int intersectionSize = getResultIntersection(bcnf,bcnf2,normi,normi2, dateOfLast);
			statWriter.println(intersectedFds.size() + "," + unfilteredFdsForLast.size());
			statWriter.println("#FDsInLastSnapshot,#fdsInIntersection,#chosenKeyFDsWithUnfiltered,#chosenKeyFDsWithIntersection,#chosenKeyFDsInBoth");
			statWriter.println(unfilteredFdsForLast.size() + "," + intersectedFds.size() + "," + collectedResults.size() + "," + resultsWithOriginalFD.size() + ","+ intersectionSize);
			statWriter.close();
		}
	}

	private static int getResultIntersection(List<Schema> collectedResults, List<Schema> resultsWithOriginalFD,Normi normi1,Normi normi2, LocalDate version) {
		TemporalSchema temporalSchema = TemporalSchema.load(datasetID);
		scala.collection.immutable.Map<String, Attribute> colNameToAttributeState = temporalSchema.nameToAttributeState(version);
		HashSet<Set<String>> pkSet1 = new HashSet<Set<String>>();
		HashSet<Set<String>> pkSet2 = new HashSet<Set<String>>();
		for (Schema schema : collectedResults) {
			String pkString = normi1.BitSetAttributesToString(schema.getPrimaryKey().getLhs());
			Set<Attribute> pk = getPrimaryKey(datasetID, colNameToAttributeState, pkString);
			HashSet<String> pkS = new HashSet<String>();
			pk.foreach(a -> pkS.add(a.name()));
			pkSet1.add(pkS);
		}
		for (Schema schema : resultsWithOriginalFD) {
			String pkString = normi2.BitSetAttributesToString(schema.getPrimaryKey().getLhs());
			Set<Attribute> pk = getPrimaryKey(datasetID, colNameToAttributeState, pkString);
			HashSet<String> pkS = new HashSet<String>();
			pk.foreach(a -> pkS.add(a.name()));
			pkSet2.add(pkS);
		}
		return pkSet1.intersect(pkSet2).size();
	}

	private static int countLines(Path datasetVersionFD) {
		return 0;
	}

	public static Map<BitSet, BitSet> getFdsForFile(String datasetID,Path datasetVersionCSV,Path datasetVersionFD){
		conf.inputDatasetName = datasetVersionCSV.getFileName().toString().replace(".csv", "");
		conf.inputFolderPath=datasetVersionCSV.getParent()+File.separator;
		try {
			Normi normi = new Normi();
			configureNormi(conf, datasetVersionFD.getParent().getParent().toString(), normi);
			return normi.discoverFds(maxFDLHSSize);
		}
		catch (AlgorithmExecutionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static ResultCache configureNormi(Config conf, String tempResultDir, Normi normi) throws FileNotFoundException, AlgorithmConfigurationException {
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
		return resultReceiver;
	}

	private static void writeToFile(FileWriter writer, String line) {
		try {
			writer.write(String.format("%s%n", line));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void writeToFile(FileWriter writer,Normi normi,String DS, LocalDate version, int decomposedTableID, Schema decomposedTable) {
		//replace here if you need to change JSON format

		//method-1  basic_statistics as JSON
		//JSONObject jo = new JSONObject(r);
		String schemaString = normi.BitSetAttributesToString(decomposedTable.getAttributes());
		String pkString = normi.BitSetAttributesToString(decomposedTable.getPrimaryKey().getLhs());
		java.util.Set<String> fkStrings = decomposedTable.getReferencedSchemata().stream()
			.map(s -> normi.BitSetAttributesToString(s.getPrimaryKey().getLhs()))
				.collect(Collectors.toSet());

		//method-2 simplified JSON
		//because we use only FDs within the same tables i removed the table id from all fields but the id
		JSONObject jo = new JSONObject();
		//parse apart id and version:
		TemporalSchema temporalSchema = TemporalSchema.load(DS);
		scala.collection.immutable.Map<String, Attribute> colNameToAttributeState = temporalSchema.nameToAttributeState(version);
		ArrayBuffer<Attribute> schema = getSchemaList(DS,colNameToAttributeState, schemaString);
		scala.collection.Set<Attribute> pk = getPrimaryKey(DS,colNameToAttributeState,pkString);
		scala.collection.Set<scala.collection.Set<Attribute>> fks = getForeignKey(DS,colNameToAttributeState,fkStrings);
		DecomposedTable res = new DecomposedTable(DS,version,decomposedTableID, schema,pk,fks.toSet());
		System.out.println(res.getSchemaStringWithIds());
		res.appendToWriter(writer,false,true,true);
	}

	private static Set<Set<Attribute>> getForeignKey(String DS, scala.collection.immutable.Map<String, Attribute> colNameToAttributeState, java.util.Set<String> fkStrings) {
		HashSet<Set<Attribute>> fks = new HashSet<>();
		for (String fkString : fkStrings) {
			HashSet<Attribute> curFk = new HashSet<>();
			String fkStr = fkString.replace(".csv","").replace(DS+".","");
			Arrays.asList(fkStr.split(",")).stream().map(s -> s.trim().split("\\.")[1]).forEach(s -> curFk.add(colNameToAttributeState.get(s).get()));
			fks.add(curFk);
		}
		return fks;
	}

	private static Set<Attribute> getPrimaryKey(String DS, scala.collection.immutable.Map<String, Attribute> colNameToAttributeState, String pkInputString) {
		HashSet<Attribute> pk = new HashSet<>();
		String pkStr = pkInputString.replace(".csv","").replace(DS+".","");
		Arrays.asList(pkStr.split(",")).stream().map(s -> s.trim().split("\\.")[1]).forEach(s -> pk.add(colNameToAttributeState.get(s).get()));
		return pk;
	}

	private static ArrayBuffer<Attribute> getSchemaList(String DS, scala.collection.immutable.Map<String, Attribute> colNameToAttributeState, String schemaStringWithFileEnding) {
		ArrayBuffer<Attribute> schemaAsScala = new ArrayBuffer<Attribute>();
		String schemaString = schemaStringWithFileEnding.replace(".csv","").replace(DS+".","");
		//schemaString = schemaString.substring(1,schemaString.length()-1);
		List<String> schema = Arrays.asList(schemaString.split(","));
		schema.forEach(s -> {
			String colname = s.split("\\.")[1];
			schemaAsScala.addOne(colNameToAttributeState.get(colname).get());
		});
		return schemaAsScala;
	}
}
