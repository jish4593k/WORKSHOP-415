import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class DataProcessor {

    public static void main(String[] args) {
        processAndGenerateCsv("your_data.csv", "JK", "2022-2023");
        generateYearwisePopularityCsv("your_data.csv", "output.csv", "JK");
    }

    private static void processAndGenerateCsv(String csvName, String state, String yearRange) {
        try (BufferedReader reader = new BufferedReader(new FileReader(csvName))) {
            List<String> names = reader.lines()
                    .skip(1) // Skip header
                    .map(line -> line.split(",")[3]) // Assuming 'elector_name' is in the fourth column
                    .flatMap(name -> Arrays.stream(name.split(" ")))
                    .collect(Collectors.toList());

            Map<String, Integer> nameDict = new HashMap<>();
            names.forEach(name -> {
                for (String part : name.split(" ")) {
                    nameDict.put(part, nameDict.getOrDefault(part, 0) + 1);
                }
            });

            List<Map.Entry<String, Integer>> sortedNames = nameDict.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toList());

            List<List<String>> namesWithSuffixArray = new ArrayList<>();
            int total = 0;
            for (Map.Entry<String, Integer> entry : sortedNames) {
                total += entry.getValue();
            }

            for (Map.Entry<String, Integer> entry : sortedNames) {
                List<String> nameData = new ArrayList<>();
                nameData.add(entry.getKey());
                nameData.add(String.valueOf(entry.getValue()));
                nameData.add(String.valueOf(total));
                nameData.add(yearRange);
                nameData.add(state);
                total -= entry.getValue();
                namesWithSuffixArray.add(nameData);
            }

            try (CSVWriter writer = new CSVWriter(new FileWriter(csvName.split("\\.")[0] + "_popularity.csv"))) {
                writer.writeNext(new String[]{"Name", "Popularity", "Suffix Sum", "Year Range", "State"});
                namesWithSuffixArray.forEach(data -> writer.writeNext(data.toArray(new String[0])));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<Integer, List<String[]>> divideAccordingToBirthYear(String csv) {
        Map<Integer, List<String[]>> myDict = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csv))) {
            reader.lines()
                    .skip(1) // Skip header
                    .forEach(line -> {
                        String[] row = line.split(",");
                        int age = Integer.parseInt(row[7]);
                        int currYear = Integer.parseInt(row[12]);
                        int birthYear = currYear - age;
                        int fileNo = (int) Math.ceil((birthYear - 1900) / 5.0);

                        myDict.computeIfAbsent(fileNo, k -> new ArrayList<>()).add(row);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return myDict;
    }

    private static String noToFile(int fileNo) {
        return (1900 + (fileNo * 5)) + "_" + (1900 + fileNo * 5 + 4) + ".csv";
    }

    private static void generateYearwisePopularityCsv(String csv, String opCsvName, String state) {
        Map<Integer, List<String[]>> myDict = divideAccordingToBirthYear(csv);

        for (Map.Entry<Integer, List<String[]>> entry : myDict.entrySet()) {
            int key = entry.getKey();
            String filename = noToFile(key);
            try (CSVWriter writer = new CSVWriter(new FileWriter(filename))) {
                writer.writeNext(new String[]{
                      
                });

                entry.getValue().forEach(writer::writeNext);
                processAndGenerateCsv(filename, state, filename.split("\\.")[0]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        List<String[]> concatenatedDataFrame = myDict.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        try (CSVWriter writer = new CSVWriter(new FileWriter(opCsvName))) {
            writer.writeAll(concatenatedDataFrame);
        } catch (IOException e) {
            e.printStackTrace();
        }

        myDict.keySet().stream()
                .map(key -> new File(noToFile(key)))
                .filter(File::exists)
                .forEach(File::delete);
    }
}
