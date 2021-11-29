package tbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Scanner;


public class Converter {
    public static String readFile(String filePath) throws FileNotFoundException {
        StringBuilder sb = new StringBuilder();
        File fileObj = new File(filePath);
        Scanner sc = new Scanner(fileObj);
        while (sc.hasNextLine()) {
            sb.append(sc.nextLine());
        }
        sc.close();

        return sb.toString();
    }

    public static void createSSTable(String dataPath, String outDir, Table table) throws IOException, CsvValidationException, ParseException {
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        System.out.println("********************************************************************");
        System.out.println("Generating sstable in path " + dataPath + "for table " +
                table.getTableName() + " output: " + outDir);

        builder.inDirectory(outDir)
                .forTable(table.getSchema())
                .using(table.getStmt())
                .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter writer = builder.build();

        CSVReader reader = new CSVReaderBuilder(new FileReader(dataPath))
                .withCSVParser(new CSVParserBuilder().withSeparator('|').build()).build();

        String [] nextLine;
        try {
            while ((nextLine = reader.readNext()) != null) {
                writer.addRow(table.convert(nextLine));
            }
        }
        catch(Exception e) {
            writer.close();
            throw e;
        }
        writer.close();

    }

    public static void main(String[] args) throws IOException, CsvValidationException, InvalidRequestException, ParseException {
        String tablesPath = args[0];
        String fileContent = readFile(tablesPath);
        ObjectMapper mapper = new ObjectMapper();
        InputFile inputFile = mapper.readValue(fileContent, InputFile.class);

        HashMap<String, TableSchema> tableSchemas = new HashMap<>();


        for(TableInfo info: inputFile.infos) {
            Path path = Paths.get(info.outputDir+"/"+info.outputDirName);
            Files.createDirectories(path);

            TableSchema tableSchema;
            if(tableSchemas.containsKey(info.tableName)) {
                tableSchema = tableSchemas.get(info.tableName);
            }
            else {
                String schemaContent = readFile(info.tableSchemaSrcPath);
                tableSchema = mapper.readValue(schemaContent, TableSchema.class);
                tableSchemas.put(info.tableName, tableSchema);
            }

            createSSTable(info.dataSrcPath, path.toString(), tableSchema);

        }

    }
}
