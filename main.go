package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type Response struct {
	TotalItems     int `json:"total_items"`
	TotalCategories int `json:"total_categories"`
	TotalPrice      int `json:"total_price"`
}

var db *sql.DB

func main() {
	var err error
	connStr := "host=0.0.0.0 port=5432 user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	http.HandleFunc("/api/v0/prices", handlePrices)
	http.ListenAndServe(":8080", nil)
}

func handlePrices(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		handleGet(w, r)
	} else if r.Method == http.MethodPost {
		handlePost(w, r)
	}else {
		http.Error(w, "Метод не реализован", http.StatusMethodNotAllowed)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Ошибка чтения запроса", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "Не удалось разархивировать архив", http.StatusBadRequest)
		return
	}

	var totalItems, totalCategories, totalPrice int
	categorySet := make(map[string]bool)
	
	for _, file := range zipReader.File {
		if !strings.Contains(file.Name, ".csv") {
			continue
		}
		f, _ := file.Open()

		reader := csv.NewReader(f)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			// Пропуск первой строки
			if record[0] == "id" {
				continue
			}
			
			id, _ := strconv.ParseInt(record[0], 10, 64)
			name := record[1]
			category := record[2]
			price, _ := strconv.ParseFloat(record[3], 64)
			createdAt := record[4]
			var priceInt int = int(math.Round(price))

			_, _ = db.Exec("INSERT INTO prices (id, name, category, price, created_at) VALUES ($1, $2, $3, $4, $5)", id, name, category, priceInt, createdAt)

			totalItems = totalItems + 1
			totalPrice = totalPrice + priceInt
			categorySet[category] = true
		}
	}
	totalCategories = len(categorySet)
	response := Response{
		TotalItems: totalItems,
		TotalCategories: totalCategories,
		TotalPrice: totalPrice,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, created_at, name, category, price FROM prices")
	if err != nil {
		http.Error(w, "Ошибка получения данных", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	file_data, _ := os.Create("data.csv")

	writer := csv.NewWriter(file_data)
	writer.Write([]string{"id", "name", "category", "price", "created_at"})
	for rows.Next() {
		var id, price int
		var createdAt, name, category string
		rows.Scan(&id, &name, &category, &price, &createdAt)
		writer.Write([]string{
			strconv.FormatInt(int64(id), 10),
			name,
			category,
			strconv.FormatInt(int64(price), 10),
			createdAt,
		})
	}
	writer.Flush()
	file_data.Close()
	zipFile, _ := os.CreateTemp("", "data-*.zip")
	zipWriter := zip.NewWriter(zipFile)
	f, _ := zipWriter.Create(filepath.Base(file_data.Name()))
	csvFile, _ := os.Open(file_data.Name())
	_, _ = io.Copy(f, csvFile)
	csvFile.Close()
	zipWriter.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	http.ServeFile(w, r, zipFile.Name())
}