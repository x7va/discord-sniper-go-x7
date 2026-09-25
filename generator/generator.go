package generator

import (
	"fmt"
	"log"
	"os"
)

const (
	// Character set: lowercase letters (a-z), dot (.), and underscore (_)
	charset = "abcdefghijklmnopqrstuvwxyz._"
	// Length of each combination
	combinationLength = 4
)

// GenerateTargets generates all possible 4-character combinations and writes them to targets.txt
func GenerateTargets() {
	// Generate all possible 4-character combinations
	combinations := generateCombinations(charset, combinationLength)

	// Write combinations to targets.txt
	filename := "targets.txt"
	count, err := writeCombinationsToFile(combinations, filename)
	if err != nil {
		log.Fatalf("Failed to write combinations to file: %v", err)
	}

	// Log success message
	fmt.Printf("Successfully generated and wrote %d target variations to %s\n", count, filename)
}

// generateCombinations generates all possible combinations of the given length
// using the provided character set.
func generateCombinations(chars string, length int) []string {
	if length == 0 {
		return []string{""}
	}

	var combinations []string
	generateHelper(chars, length, "", &combinations)
	return combinations
}

// generateHelper is a recursive helper function to generate combinations.
func generateHelper(chars string, length int, current string, result *[]string) {
	if length == 0 {
		*result = append(*result, current)
		return
	}

	for _, char := range chars {
		generateHelper(chars, length-1, current+string(char), result)
	}
}

// writeCombinationsToFile writes the combinations to a file, one per line.
func writeCombinationsToFile(combinations []string, filename string) (int, error) {
	file, err := os.Create(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	count := 0
	for _, combo := range combinations {
		_, err := fmt.Fprintln(file, combo)
		if err != nil {
			return count, fmt.Errorf("failed to write combination: %w", err)
		}
		count++
	}

	return count, nil
}
