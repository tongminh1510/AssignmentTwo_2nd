package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strconv"
)

type Film struct {
	Name     string `json:"name"`
	Year     uint64 `json:"year"`
	Genre    string `json:"genre"`
	Director User   `json:"director"`
	Cast     []User `json:"cast"`
}

type User struct {
	Name string `json:"user_name"`
	DOB  string `json:"birth_year"`
	Role string `json:"role"`
}

type FilmUser struct {
	FilmID int `json:"film_id"`
	UserID int `json:"user_id"`
}

var db *sql.DB

func main() {
	cfg := mysql.Config{
		User:                 "root",
		Passwd:               "minhtom1510",
		Net:                  "tcp",
		Addr:                 "127.0.0.1:3306",
		DBName:               "FILMS",
		AllowNativePasswords: true,
	}

	// Get a database handle.
	var err error
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected!")

	router := mux.NewRouter()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	// Create one new movie
	router.HandleFunc("/movie/", CreateMovie).Methods("POST")

	// Get one movie
	router.HandleFunc("/movie/{film_id}", getMovieDetails).Methods("GET")

	// Delete one movie
	router.HandleFunc("/movie/{film_id}", deleteOneMovie).Methods("DELETE")

	// Update one movie
	router.HandleFunc("/movie/{film_id}", updateOneMovie).Methods("PUT")

	// Update user
	router.HandleFunc("/movie/{film_id}/{user_id}", updateUserData).Methods("PUT")

	tx.Commit()
	defer db.Close()

	fmt.Println("Server at 8080")
	log.Fatal(http.ListenAndServe(":8080", router))

}

func CreateMovie(w http.ResponseWriter, r *http.Request) {
	var movie Film
	err := json.NewDecoder(r.Body).Decode(&movie) // giai ma du lieu dang json sang movie
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusBadRequest)
		return
	}

	// Lay thong tin bo phim
	filmID, err := createFilm(movie.Name, movie.Year, movie.Genre)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Lay thong tin cua Director va add thong tin cua Director len DB
	directorID, err := createUser(movie.Director)
	if err != nil {
		http.Error(w, "Cannot take director information", http.StatusInternalServerError)
		return
	}

	// Lay thong tin cua dan cast
	var castMembersID []int
	for _, mem := range movie.Cast {
		tmp, err := createUser(mem)
		if err != nil {
			http.Error(w, "Cannot create cast member", http.StatusInternalServerError)
			return
		}
		castMembersID = append(castMembersID, tmp)
	}

	// luu thong tin ve bo phim va dao dien trong film user
	_, err = createFilmUser(filmID, directorID, "Director")
	if err != nil {
		http.Error(w, "Cannot return movie and cast and director information", http.StatusInternalServerError)
		return
	}

	// luu thong tin ve bo phim va dien vien trong film user
	for _, castID := range castMembersID {
		_, err = createFilmUser(filmID, castID, "Actor")
		if err != nil {
			http.Error(w, "Cannot save actor information", http.StatusInternalServerError)
			return
		}
	}
}

func createUser(user User) (int, error) {
	result, err := db.Exec("INSERT INTO USER(FULL_NAME, DOB) VALUES (?, ?)", user.Name, user.DOB)
	if err != nil {
		return 0, err
	}

	// Lấy ID của cast được tạo mới
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	// Trả về ID của cast được tạo mới
	return int(id), nil
}

func createFilm(name string, year uint64, genre string) (int, error) {
	result, err := db.Exec("INSERT INTO FILM(FILM_NAME, RELEASE_YEAR, GENRE) VALUES (?, ?, ?)", name, year, genre)
	if err != nil {
		return 0, err
	}

	// Lấy ID của phim dùng được tạo mới
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	// Trả về ID của film được tạo mới
	return int(id), nil
}

func createFilmUser(filmID int, userID int, role string) (int, error) {
	result, err := db.Exec("INSERT INTO FILM_USER(film_id, user_id, role) VALUES (?, ?, ?)", filmID, userID, role)
	if err != nil {
		return 0, err
	}

	// Lấy ID của cast dùng được tạo mới
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	// Trả về ID của film được tạo mới
	return int(id), nil
}

func getMovieDetails(w http.ResponseWriter, r *http.Request) {
	// get name from URL
	params := mux.Vars(r)
	filmID, err := strconv.Atoi(params["film_id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	film, err := getFilmData(filmID) // get film information
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(film)
}

func getFilmData(filmID int) (Film, error) {
	var (
		name     string
		year     uint64
		genre    string
		director User
	)
	row := db.QueryRow("SELECT FILM_NAME, RELEASE_YEAR, GENRE FROM FILM WHERE FILM_ID = ?", filmID)
	err := row.Scan(&name, &year, &genre)
	if err != nil {
		return Film{}, err
	}

	err = db.QueryRow("SELECT u.FULL_NAME, u.DOB, fu.role from USER u join FILM_USER fu on u.USER_ID = fu.user_id where fu.film_id = ? and fu.role = 'Director'", filmID).Scan(&director.Name, &director.DOB, &director.Role)
	if err != nil {
		return Film{}, err
	}

	rows, err := db.Query("SELECT u.FULL_NAME, u.DOB, fu.role from USER u join FILM_USER fu on u.USER_ID = fu.user_id where fu.film_id = ? and fu.role = 'Actor'", filmID)
	if err != nil {
		return Film{}, err
	}
	defer rows.Close()

	var actors []User
	for rows.Next() {
		var actor User
		err := rows.Scan(&actor.Name, &actor.DOB, &actor.Role)
		if err != nil {
			return Film{}, err
		}
		actors = append(actors, actor)
	}
	err = rows.Err()
	if err != nil {
		return Film{}, err
	}

	film := Film{
		Name:     name,
		Year:     year,
		Genre:    genre,
		Director: director,
		Cast:     actors,
	}

	return film, nil
}

func deleteOneMovie(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	filmID, err := strconv.Atoi(params["film_id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	err = deleteFilmData(filmID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = deleteUsersByIDandRole(filmID, "Director")
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = deleteUsersByIDandRole(filmID, "Actor")
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = deleteFilmUserByID(filmID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filmID)
}

func deleteFilmData(filmID int) error {
	// check whether the movie exist
	_, err := getFilmData(filmID)
	if err != nil {
		return err
	}

	// delete the film information
	_, err = db.Exec("DELETE FROM FILM WHERE FILM_ID = ?", filmID)
	if err != nil {
		return err
	}
	return nil
}

func deleteUsersByIDandRole(filmID int, role string) error {
	_, err := db.Exec("DELETE u.* from user u where u.USER_ID in (select user_id from film_user fu where fu.film_ID = ?)", filmID)
	if err != nil {
		return err
	}
	return nil
}

func deleteFilmUserByID(filmID int) error {
	_, err := db.Exec("DELETE FROM FILM_USER WHERE film_id = ?", filmID)
	if err != nil {
		return err
	}
	return nil
}

func updateOneMovie(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	filmID, err := strconv.Atoi(params["film_id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	var film Film
	err = json.NewDecoder(r.Body).Decode(&film)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Cập nhật thông tin bộ phim trong bảng FILM
	_, err = db.Exec("UPDATE FILM SET FILM_NAME = ?, RELEASE_YEAR = ?, GENRE= ? WHERE FILM_ID = ?", film.Name, film.Year, film.Genre, filmID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func updateUserData(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	filmID, err := strconv.Atoi(params["film_id"])
	userID, err := strconv.Atoi(params["user_id"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	var newUser User
	err = json.NewDecoder(r.Body).Decode(&newUser) // giai ma du lieu dang json sang user
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusBadRequest)
		return
	}

	// check whether the movie exist
	_, err = getUserData(filmID, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = updateUserByID(filmID, userID, newUser)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = updateFilmUserByData(filmID, userID, newUser)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func getUserData(filmID int, userID int) (User, error) {
	var user User

	rows, err := db.Query("SELECT film_id, user_id from film_user where film_ID = ? and user_id = ?", filmID, userID)
	if err != nil {
		return User{}, err
	}
	defer rows.Close()

	if !rows.Next() {
		return User{}, errors.New("no matching rows found")
	}

	err = db.QueryRow("SELECT full_name, dob from user where user_id = ?", userID).Scan(&user.Name, &user.DOB)
	if err != nil {
		return User{}, err
	}

	return user, nil
}

func updateUserByID(filmID int, userID int, user User) error {
	_, err := db.Exec("update user u set u.FULL_NAME = ?, u.DOB = ? where u.USER_ID = ? and (SELECT film_id from FILM_USER fu where fu.user_id = ?) = ?", user.Name, user.DOB, userID, userID, filmID)
	if err != nil {
		return err
	}
	return nil
}

func updateFilmUserByData(filmID int, userID int, user User) error {
	_, err := db.Exec("update film_user fu set fu.Role = ? where fu.user_id = ? and fu.film_id = ?", user.Role, userID, filmID)
	if err != nil {
		return err
	}
	return nil
}
