package de.metanome.algorithms.normalize;


import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithms.normalize.aspects.NormiConversion;
import de.metanome.algorithms.normalize.aspects.NormiPersistence;
import de.metanome.algorithms.normalize.config.Config;
import de.metanome.algorithms.normalize.fddiscovery.FdDiscoverer;
import de.metanome.algorithms.normalize.fddiscovery.HyFDFdDiscoverer;
import de.metanome.backend.input.file.DefaultFileInputGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main_FDs {

    public static void main(String[] args) {
        Config conf = new Config();
        conf.inputFolderPath = args[0];
        conf.measurementsFolderPath = args[1];
        System.out.println();
        try (Stream<Path> walk = Files.walk(Paths.get(conf.inputFolderPath))) {

            List<Path> result = walk
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(f -> Long.valueOf(f.toFile().length())))
                    .collect(Collectors.toList());

            result.forEach(dataset -> {
                conf.inputDatasetName = dataset.getFileName().toString().replace(".csv", "");
                conf.inputFolderPath=dataset.getParent()+File.separator;
                try {
                    execute(conf);
                }catch (AlgorithmExecutionException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void execute(Config conf) throws AlgorithmExecutionException {
        RelationalInput relationalInput = null;

        RelationalInputGenerator inputGenerator = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                conf.inputFolderPath + conf.inputDatasetName + conf.inputFileEnding, true,
                conf.inputFileSeparator, conf.inputFileQuotechar, conf.inputFileEscape, conf.inputFileStrictQuotes,
                conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader, conf.inputFileSkipDifferingLines, conf.inputFileNullString));

        System.out.println();
        System.out.println("///// Initialization /////");
        System.out.println();

        try {
            relationalInput = inputGenerator.generateNewCopy();
        } catch (AlgorithmExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Reading table metadata ...");
        String tableName = relationalInput.relationName();// TODO: Für schema experiments: .replaceAll("\\.csv|\\.tsv|\\.txt", "");
        List<String> attributeNames = relationalInput.columnNames();
        List<ColumnIdentifier> columnIdentifiers = attributeNames.stream().map(name -> new ColumnIdentifier(tableName, name)).collect(Collectors.toCollection(ArrayList::new));

        try {
            relationalInput.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgorithmExecutionException(e.getMessage());
        }

        System.out.println("Processing table metadata ...");
        int columnIdentifierNumber = 0;
        Map<ColumnIdentifier, Integer> name2number = new HashMap<>();
        Map<Integer, ColumnIdentifier> number2name = new HashMap<>();
        for (ColumnIdentifier identifier : columnIdentifiers) {
            name2number.put(identifier, Integer.valueOf(columnIdentifierNumber));
            number2name.put(Integer.valueOf(columnIdentifierNumber), identifier);
            columnIdentifierNumber++;
        }

        String[] parts = conf.inputFolderPath.split(File.separator);
        String tempResultsPath = "temp" +File.separator + parts[1]+File.separator+ tableName + "-hyfd.txt";

        NormiConversion converter = new NormiConversion(columnIdentifiers, name2number, number2name);
        NormiPersistence persister = new NormiPersistence(columnIdentifiers);


        System.out.println(">>> " + tableName + " <<<");

        System.out.println();
        System.out.println("///// FD-Discovery ///////");
        System.out.println();

        FdDiscoverer fdDiscoverer = new HyFDFdDiscoverer(converter, persister, tempResultsPath);
        Map<BitSet, BitSet> fds = fdDiscoverer.calculateFds(inputGenerator, Boolean.valueOf(true), true);

        // Statistics
        int numFds = (int) fds.values().stream().mapToLong(BitSet::cardinality).sum();
        float avgFdsLhsLength = fds.entrySet().stream().mapToLong(entry -> entry.getKey().cardinality() * entry.getValue().cardinality()).sum() / (float) numFds;
        float avgFdsRhsLength = 1.0f;

        // Statistics
        int numAggregatedFds = fds.keySet().size();
        float avgAggregatedFdsLhsLength = fds.keySet().stream().mapToLong(BitSet::cardinality).sum() / (float) numAggregatedFds;
        float avgAggregatedFdsRhsLength = fds.values().stream().mapToLong(BitSet::cardinality).sum() / (float) numAggregatedFds;

        System.out.println();
        System.out.println("# FDs: " + numFds + " (avg lhs size: " + avgFdsLhsLength + "; avg rhs size: " + avgFdsRhsLength + ")");
        System.out.println("# aggregated FDs: " + numAggregatedFds + " (avg lhs size: " + avgAggregatedFdsLhsLength + "; avg rhs size: " + avgAggregatedFdsRhsLength + ")");

    }

}
