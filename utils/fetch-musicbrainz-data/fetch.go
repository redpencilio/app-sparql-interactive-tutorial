package main

import (
	"compress/gzip"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "user=musicbrainz password=musicbrainz dbname=musicbrainz_db sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	generateArtistsTrig(db, "artists.trig.gz")
	generateMembershipsTrig(db, "memberships.trig.gz")
	generateDiscographiesTrig(db, "discographies.trig.gz")

	log.Println("All done!")

	defer db.Close()
}

func generateArtistsTrig(db *sql.DB, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	writer := gzip.NewWriter(file)

	writer.Write([]byte("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"))
	writer.Write([]byte("@prefix mo: <http://purl.org/ontology/mo/> .\n"))
	writer.Write([]byte("@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"))
	writer.Write([]byte("@prefix mba: <https://musicbrainz.org/artist/> .\n"))
	writer.Write([]byte("<http://mu.semte.ch/graphs/music> {\n"))

	previousGid := ""
	count := 0
	log.Printf("[%s][BEGIN]: writing triples to disk", fileName)
	fetchArtists(db, func(gid, name, artist_type, area, gender *string) {
		if *gid != previousGid {
			if previousGid != "" {
				// end previous triple if not first time
				writer.Write([]byte(".\n"))
				count += 1
				if count%10000 == 0 {
					log.Printf("[%s][PROGRESS]: wrote %d triples to disk", fileName, count)
				}
			}

			// Artist type
			writer.Write([]byte(fmt.Sprintf("mba:%s rdf:type mo:MusicArtist ", *gid)))
			if *artist_type == "Group" {
				writer.Write([]byte(", mo:MusicGroup "))
			}

			// Artist name
			escaped_name := strings.Replace(*name, "\\", "\\\\", -1)
			escaped_name = strings.Replace(escaped_name, "\"", "\\\"", -1)
			writer.Write([]byte(fmt.Sprintf("; foaf:name \"%s\" ", escaped_name)))

			// Gender
			if *gender != "" {
				writer.Write([]byte(fmt.Sprintf("; foaf:gender \"%s\" ", *gender)))
			}

			// Area
			if *area != "" {
				writer.Write([]byte(fmt.Sprintf("; foaf:based_near \"%s\" ", *area)))
			}
			previousGid = *gid
		} else if *area != "" {
			// There can be multiple rows for area (e.g. artist based near Antwerp -> artist based near Belgium)
			writer.Write([]byte(fmt.Sprintf(",\"%s\" ", *area)))
		}
	})
	log.Printf("[%s][FINAL]: wrote %d triples to  disk", fileName, count)
	writer.Write([]byte(".\n}\n"))
	writer.Close()
	file.Close()
}

func generateMembershipsTrig(db *sql.DB, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	writer := gzip.NewWriter(file)

	writer.Write([]byte("@prefix mo: <http://purl.org/ontology/mo/> .\n"))
	writer.Write([]byte("@prefix mba: <https://musicbrainz.org/artist/> .\n"))
	writer.Write([]byte("<http://mu.semte.ch/graphs/music> {\n"))

	previousGid := ""
	count := 0
	log.Printf("[%s][BEGIN]: writing triples to disk", fileName)
	fetchMemberships(db, func(member_gid, band_gid *string) {
		if *member_gid != previousGid {
			if previousGid != "" {
				// end previous triple if not first time
				writer.Write([]byte(".\n"))
				count += 1
				if count != 0 && count%100000 == 0 {
					log.Printf("[%s][PROGRESS]: wrote %d triples to disk", fileName, count)
				}
			}

			writer.Write([]byte(fmt.Sprintf("mba:%s mo:member_of mba:%s ", *member_gid, *band_gid)))
			previousGid = *member_gid
		} else {
			// There can be multiple rows for memberships (e.g. artist is part of multiple bands) we want to put these in one line
			writer.Write([]byte(fmt.Sprintf(", mba:%s ", *band_gid)))
		}
	})

	log.Printf("[%s][FINAL]: wrote %d triples to  disk", fileName, count)
	writer.Write([]byte("}\n"))
	writer.Close()
	file.Close()
}

func generateDiscographiesTrig(db *sql.DB, fileName string) {
	createNewFile := func(baseFileName *string, counter int) (*os.File, *gzip.Writer) {
		file, err := os.Create(fmt.Sprintf("%02d-%s", counter, *baseFileName))
		if err != nil {
			log.Fatal(err)
		}
		writer := gzip.NewWriter(file)

		log.Printf("[%s][BEGIN]: writing triples to disk", file.Name())

		writer.Write([]byte("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"))
		writer.Write([]byte("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"))
		writer.Write([]byte("@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"))
		writer.Write([]byte("@prefix mo: <http://purl.org/ontology/mo/> .\n"))
		writer.Write([]byte("@prefix mba: <https://musicbrainz.org/artist/> .\n"))
		writer.Write([]byte("@prefix mbt: <https://musicbrainz.org/track/> .\n"))
		writer.Write([]byte("@prefix mbr: <https://musicbrainz.org/recording/> .\n"))
		writer.Write([]byte("<http://mu.semte.ch/graphs/music> {\n"))

		return file, writer
	}

	closeFile := func(file *os.File, writer *gzip.Writer, counter *int) {
		log.Printf("[%s][FINAL]: closing file", file.Name())
		writer.Write([]byte("}\n"))
		writer.Close()
		file.Close()

		*counter += 1
	}

	counter := 0
	file, writer := createNewFile(&fileName, counter)
	count := 0
	fetchDiscographies(db, func(artist_gid, artist_type, track_gid, track_name *string, track_number, track_duration *int64, recording_gid, recording_name *string) {
		escaped_track_name := strings.Replace(*track_name, "\\", "\\\\", -1)
		escaped_track_name = strings.Replace(escaped_track_name, "\"", "\\\"", -1)

		escaped_recording_name := strings.Replace(*recording_name, "\\", "\\\\", -1)
		escaped_recording_name = strings.Replace(escaped_recording_name, "\"", "\\\"", -1)

		writer.Write([]byte(fmt.Sprintf("mbr:%s rdf:type mo:Record, mo:MusicalManifestation ", *recording_gid)))
		writer.Write([]byte(fmt.Sprintf("; rdfs:label \"%s\"", escaped_recording_name)))
		writer.Write([]byte(fmt.Sprintf("; mo:track mbt:%s .\n", *track_gid)))
		count += 1
		if count != 0 && count%100000 == 0 {
			log.Printf("[%s][PROGRESS]: wrote %d triples to disk", file.Name(), count)
		}
		if count != 0 && count%10000000 == 0 {
			closeFile(file, writer, &counter)
			file, writer = createNewFile(&fileName, counter)
		}

		writer.Write([]byte(fmt.Sprintf("mbt:%s rdf:type mo:Track , mo:MusicalManifestation ", *track_gid)))
		writer.Write([]byte(fmt.Sprintf("; rdfs:label \"%s\"", escaped_track_name)))
		if *track_number > 0 {
			writer.Write([]byte(fmt.Sprintf("; mo:track_number %d ", *track_number)))
		}
		if *track_duration > 0 {
			writer.Write([]byte(fmt.Sprintf("; mo:duration %d ", *track_duration)))
		}
		writer.Write([]byte(".\n"))
		count += 1
		if count != 0 && count%100000 == 0 {
			log.Printf("[%s][PROGRESS]: wrote %d triples to disk", file.Name(), count)
		}
		if count != 0 && count%10000000 == 0 {
			closeFile(file, writer, &counter)
			file, writer = createNewFile(&fileName, counter)
		}

		writer.Write([]byte(fmt.Sprintf("mba:%s foaf:made mbr:%s , mbt:%s.\n", *artist_gid, *recording_gid, *track_gid)))
		count += 1
		if count != 0 && count%1000000 == 0 {
			log.Printf("[%s][PROGRESS]: wrote %d triples to disk", file.Name(), count)
		}
		if count != 0 && count%1000000 == 0 {
			closeFile(file, writer, &counter)
			file, writer = createNewFile(&fileName, counter)
		}

		if *artist_type == "Person" {
			writer.Write([]byte(fmt.Sprintf("mba:%s rdf:type mo:SoloMusicArtist .\n", *artist_gid)))
			count += 1
			if count != 0 && count%100000 == 0 {
				log.Printf("[%s][PROGRESS]: wrote %d triples to disk", file.Name(), count)
			}
			if count != 0 && count%10000000 == 0 {
				closeFile(file, writer, &counter)
				file, writer = createNewFile(&fileName, counter)
			}
		}
	})

	log.Printf("[%s][FINAL]: wrote %d triples to  disk", file.Name(), count)
	writer.Write([]byte("}\n"))
	writer.Close()
	file.Close()
}

func fetchArtists(db *sql.DB, process func(gid, name, artist_type, area, gender *string)) {
	var (
		gid         string
		name        string
		artist_type sql.NullString
		area        sql.NullString
		gender      sql.NullString
	)

	stmt, err := db.Prepare(`WITH RECURSIVE artist_with_all_areas (gid, name, artist_type, gender, area, area_id)
	AS (SELECT
			artist.gid as gid,
			artist.name as name,
			artist_type.name as artist_type,
			LOWER(gender.name) as gender,
			area.name as area,
			area.id as area_id
		FROM artist
		 	LEFT JOIN artist_type
			ON artist.type = artist_type.id
			LEFT JOIN area
			ON artist.area = area.id
			LEFT JOIN gender
			ON artist.gender = gender.id
		UNION
		SELECT 
			artist_with_all_areas.gid,
			artist_with_all_areas.name,
			artist_with_all_areas.artist_type,
			artist_with_all_areas.gender,
			area.name as area,
			area.id as area_id
		FROM artist_with_all_areas
			JOIN l_area_area
			ON artist_with_all_areas.area_id = l_area_area.entity1
			JOIN area
			ON l_area_area.entity0 = area.id)
	SELECT gid, name, artist_type, gender, area
	FROM artist_with_all_areas`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&gid, &name, &artist_type, &gender, &area)
		if err != nil {
			log.Fatal(err)
		}
		process(&gid, &name, &artist_type.String, &area.String, &gender.String)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}

func fetchMemberships(db *sql.DB, process func(*string, *string)) {
	var (
		member_gid string
		band_gid   string
	)

	stmt, err := db.Prepare(`SELECT
		member.gid as member_gid,
		band.gid as band_gid
	FROM artist member
	JOIN l_artist_artist ON
	member.id = l_artist_artist.entity0
	JOIN link on
	l_artist_artist.link = link.id 
	JOIN artist band on
	l_artist_artist.entity1 = band.id
	WHERE link.link_type = 103`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&member_gid, &band_gid)
		if err != nil {
			log.Fatal(err)
		}
		process(&member_gid, &band_gid)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}

func fetchDiscographies(db *sql.DB, process func(*string, *string, *string, *string, *int64, *int64, *string, *string)) {
	var (
		artist_gid     string
		artist_type    string
		track_gid      string
		track_name     string
		track_number   sql.NullInt64
		track_duration sql.NullInt64
		recording_gid  string
		recording_name string
	)
	stmt, err := db.Prepare(`SELECT
		a.gid as artist_gid,
		CASE WHEN a.type = 1 THEN 'Person'
			 ELSE 'Other'
		END AS artist_type,
		t.gid as track_gid,
		t."name" as track_name,
		t.position as track_number,
		t.length as track_duration,
		r.gid as recording_gid,
		r.name as recording_name
	FROM track t
	JOIN recording r
	ON
		r.id = t.recording
	JOIN l_artist_recording lar
	ON
		r.id = lar.entity1 
	JOIN artist a
	ON
		a.id = lar.entity0
	ORDER BY recording_gid`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		// rowsFound = true
		err := rows.Scan(&artist_gid, &artist_type, &track_gid, &track_name, &track_number, &track_duration, &recording_gid, &recording_name)
		if err != nil {
			log.Fatal(err)
		}
		process(&artist_gid, &artist_type, &track_gid, &track_name, &track_number.Int64, &track_duration.Int64, &recording_gid, &recording_name)
	}
}
