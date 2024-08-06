package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

func humanReadableSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func downloadFile(url string, maxBytes int, filePath string) error {
	fmt.Println("Starting download of the index file...")

	// Make a HEAD request to get the file size
	resp, err := http.Head(url)
	if err != nil {
		return fmt.Errorf("error making HEAD request: %v", err)
	}
	size := resp.ContentLength
	fmt.Printf("Full size of the file: %s\n", humanReadableSize(size))

	if maxBytes > 0 && size > int64(maxBytes) {
		fmt.Printf("Only the first %s will be downloaded.\n", humanReadableSize(int64(maxBytes)))
	} else {
		maxBytes = int(size)
	}

	// Make a GET request with range header
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("Range", "bytes=0-"+strconv.Itoa(maxBytes-1))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error downloading index file: %v", err)
	}
	defer resp.Body.Close()

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	fmt.Println("Starting file download...")
	buf := make([]byte, 1024)
	var totalBytes int64
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			totalBytes += int64(n)
			file.Write(buf[:n])
			percentage := float64(totalBytes) / float64(maxBytes) * 100
			fmt.Printf("\rDownloaded %s / %s (%.2f%%)", humanReadableSize(totalBytes), humanReadableSize(int64(maxBytes)), percentage)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading from response body: %v", err)
		}
	}
	fmt.Println("\nDownload completed.")
	return nil
}

func main() {
	// Define flags
	maxBytes := flag.Int("maxbytes", 0, "Maximum number of bytes to download (0 for full file)")
	skipDownload := flag.Bool("skipdownload", false, "Skip downloading the file if already available")
	flag.Parse()

	fmt.Println("Program started.")
	indexFileURL := "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-08-01_anthem_index.json.gz"
	fmt.Printf("Index file URL: %s\n", indexFileURL)

	dataFolder := "data"
	if _, err := os.Stat(dataFolder); os.IsNotExist(err) {
		err := os.Mkdir(dataFolder, 0755)
		if err != nil {
			fmt.Printf("Failed to create data folder: %v\n", err)
			return
		}
	}

	filePath := filepath.Join(dataFolder, "index_file.gz")
	var err error

	if *skipDownload {
		fmt.Println("Skipping download as per flag. Opening existing file...")
		if _, err = os.Stat(filePath); os.IsNotExist(err) {
			fmt.Printf("Failed to open existing gzip file: %v\n", err)
			return
		}
	} else {
		// Download the entire gzip file
		err = downloadFile(indexFileURL, *maxBytes, filePath)
		if err != nil {
			fmt.Printf("Failed to download index file: %v\n", err)
			return
		}
		fmt.Println("File downloaded successfully.")
	}

	fmt.Println("Program completed successfully.")
}
