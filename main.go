package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"math"
	"log"
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

func handlePost(w http.ResponseWriter, r *http.Request){
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
	
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Ошибка начала транзакции", http.StatusInternalServerError)
		return
	}
	var errExec error
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			log.Printf("Паника: %v", p)
			http.Error(w, "Внутренняя ошибка сервера", http.StatusInternalServerError)
			return
		}
		if errExec != nil {
			log.Fatalf("Откат транзакции. Ошибка %v", errExec)
			tx.Rollback()
		} else {
			log.Printf("Фиксирование транзакции")
			tx.Commit()
		}
	}()

	for _, file := range zipReader.File {
		if !strings.Contains(file.Name, ".csv") {
			continue
		}
		f, errExec := file.Open()
		if errExec != nil {
			http.Error(w, "Ошибка открытия csv-файла из архива", http.StatusInternalServerError)
			return
		}
		defer f.Close()

		reader := csv.NewReader(f)
		// Чтение файла перед работой с БД
		records, errExec := reader.ReadAll()
		if errExec != nil {
			http.Error(w, "Ошибка чтения CSV файла", http.StatusBadRequest)
			return
		}

		for _, record := range records {
		// Пропуск первой строки
			if record[0] == "id" {
				continue
			}
			// Убрал считывание id, т.к. он автоинкрементный и не требуется в запросе к бд
			name := record[1]
			category := record[2]
			price, errExec := strconv.ParseFloat(record[3], 64)
			if errExec != nil {
				http.Error(w, "Некорректные данные в поле price", http.StatusBadRequest)
				return
			}
			createdAt := record[4]
			var priceInt int = int(math.Round(price))

			_, errExec = tx.Exec("INSERT INTO prices (name, category, price, created_at) VALUES ($1, $2, $3, $4)", name, category, priceInt, createdAt)
			if errExec != nil {
				http.Error(w, "Ошибка вставки данных в базу", http.StatusInternalServerError)
				return
			}
		}
	}
	// Сбор статистики по всей таблице, а не текущей загрузке внутри транзакции
	row := tx.QueryRow("SELECT COUNT(name), COUNT(DISTINCT category), SUM(price) FROM prices")
    var totalItems, totalCategories, totalPrice int
    errExec = row.Scan(&totalItems, &totalCategories, &totalPrice)
    if errExec != nil {
        http.Error(w, "Ошибка получения данных из базы", http.StatusInternalServerError)
        return
    }
	response := Response{
		TotalItems: totalItems,
		TotalCategories: totalCategories,
		TotalPrice: totalPrice,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, name, category, price, created_at FROM prices")
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
		err := rows.Scan(&id, &name, &category, &price, &createdAt)
		if err != nil {
			http.Error(w, "Ошибка сканирования данных", http.StatusInternalServerError)
			return
		}
		writer.Write([]string{
			strconv.FormatInt(int64(id), 10),
			name,
			category,
			strconv.FormatInt(int64(price), 10),
			createdAt,
		})
	}
	if err = rows.Err(); err != nil {
		http.Error(w, "Ошибка обработки строк", http.StatusInternalServerError)
		return
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