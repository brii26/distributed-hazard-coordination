package main

// PLACEHOLDER: rough centres of Indonesia's 38 provinces, so area_name has a value until the BNPB reference table arrives (from the assistants or a teammate). Replace this slice with that table; nothing else needs to change.
// The values are approximate geographic centres picked by hand, not official centroids, so fires near a border may get the neighbouring province.
type province struct {
	name     string
	lat, lon float64
}

var provinces = []province{
	{"Aceh", 4.22, 96.91},
	{"Sumatera Utara", 2.19, 99.38},
	{"Sumatera Barat", -0.74, 100.80},
	{"Riau", 0.51, 101.81},
	{"Kepulauan Riau", 0.95, 104.45},
	{"Jambi", -1.61, 103.07},
	{"Sumatera Selatan", -3.32, 104.91},
	{"Kepulauan Bangka Belitung", -2.74, 106.44},
	{"Bengkulu", -3.79, 102.26},
	{"Lampung", -4.56, 105.41},
	{"DKI Jakarta", -6.21, 106.85},
	{"Jawa Barat", -6.89, 107.64},
	{"Banten", -6.41, 106.06},
	{"Jawa Tengah", -7.15, 110.14},
	{"DI Yogyakarta", -7.88, 110.43},
	{"Jawa Timur", -7.54, 112.24},
	{"Bali", -8.34, 115.09},
	{"Nusa Tenggara Barat", -8.65, 117.36},
	{"Nusa Tenggara Timur", -8.66, 121.08},
	{"Kalimantan Barat", -0.28, 111.48},
	{"Kalimantan Tengah", -1.68, 113.38},
	{"Kalimantan Selatan", -3.09, 115.28},
	{"Kalimantan Timur", 0.54, 116.42},
	{"Kalimantan Utara", 3.07, 116.04},
	{"Sulawesi Utara", 0.70, 124.35},
	{"Gorontalo", 0.70, 122.45},
	{"Sulawesi Tengah", -1.43, 121.45},
	{"Sulawesi Barat", -2.84, 119.23},
	{"Sulawesi Selatan", -3.67, 119.97},
	{"Sulawesi Tenggara", -4.14, 122.17},
	{"Maluku", -3.24, 130.15},
	{"Maluku Utara", 1.57, 127.81},
	{"Papua", -2.30, 138.50},
	{"Papua Barat", -2.30, 133.50},
	{"Papua Barat Daya", -1.00, 131.70},
	{"Papua Tengah", -3.90, 136.50},
	{"Papua Pegunungan", -4.10, 139.00},
	{"Papua Selatan", -7.00, 139.50},
}

// nearestProvince returns the province whose centre is closest by haversine. On a tie the first in the table wins.
func nearestProvince(lat, lon float64) string {
	best, bestKm := "", 0.0

	for _, p := range provinces {
		if km := haversineKm(lat, lon, p.lat, p.lon); best == "" || km < bestKm {
			best, bestKm = p.name, km
		}
	}

	return best
}
