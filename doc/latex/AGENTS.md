# Menulis laporan M1 (doc/latex)

Aturan di sini berlaku untuk semua berkas di `doc/latex/`. Gaya menulis ada di `notes/writing-style-guide.md`. Status tiap bagian dan fakta yang menunggu konfirmasi dicatat di GitHub issues, bukan di berkas.

## Sumber yang boleh dipakai

- `doc/TRD_M1.md`, `README.md`, kode, dan test di repo ini.
- `doc/IF4031 - Tugas Besar - M1.md` untuk isi yang diminta tiap bagian.
- Fakta yang dikonfirmasi anggota kelompok, dicatat sebagai komentar di issue bagian terkait beserta tanggalnya.

Klaim di luar itu tidak ditulis sebagai fakta. Jangan mengarang angka, tangkapan layar, hasil demo, atau hasil load test.

## Aturan tulis

- Bahasa Indonesia baku, sudut pandang orang ketiga, langsung ke inti tanpa kalimat pembuka umum.
- Istilah teknis tetap bahasa Inggris dan dicetak miring dengan `\textit{}` (*container*, *endpoint*, *rate limit*). Pakai satu istilah yang sama untuk satu hal.
- Jangan menaruh terjemahan dalam tanda kurung, misalnya `ruangan (room)`. Pilih satu kata yang paling umum dipakai.
- Jangan menulis "zero trust". Kepercayaan sistem ini ada di tepi (perimeter): panggilan gRPC internal dan pesan di broker dipercaya. Batas ini dicatat di bagian "Batasan yang Disadari".
- Bab VI sampai X sudah ditulis. Jangan diubah kecuali diminta.

## Alur kerja

1. Baca bagian target, TRD, dan kode yang relevan. Pisahkan fakta terverifikasi dari yang belum diketahui.
2. Untuk isi yang butuh keputusan atau alasan (alasan memilih, batasan, kontribusi), tanyakan dulu dalam batch kecil. Kode dan TRD tidak bisa menjawab siapa memutuskan apa.
3. Tunjukkan ringkasan terkonfirmasi, kesimpulan sementara, dan yang belum diketahui sebelum menulis.
4. Satu bab per commit. Satu bagian punya satu issue (judul `Laporan: Bab X`). Pertanyaan yang belum terjawab dan sumber fakta ditulis di komentar issue. Bagian yang belum selesai diberi komentar `% TODO[#nomor-issue]` di `.tex`, diubah menjadi `% DONE[#nomor-issue]` saat selesai, dan commit-nya memakai `Closes #nomor-issue`.
5. Sebelum commit: `tex-build` tanpa galat, `doc/latex/check-style.sh` bersih, tidak ada `<placeholder>` tersisa di bab itu, dan tiap klaim sudah dicocokkan ke TRD atau kode.
