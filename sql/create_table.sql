CREATE TABLE karyawan (
    id SERIAL PRIMARY KEY,
    nama VARCHAR(100) NOT NULL,
    jabatan VARCHAR(50),
    gaji NUMERIC(10, 2),
    tanggal_masuk DATE
);
