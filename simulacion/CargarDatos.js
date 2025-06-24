import sql from 'mssql';
import fs from 'fs';

const xmlPath = './catalogos.xml'; 

const config = {
    server: 'mssql-196019-0.cloudclusters.net',
    port: 10245,
    database: 'BASESFRANCO',
    user: 'requeSoftware',
    password: '#Jaime123',
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
};

export async function conectarDB() {
    try {
        const pool = await sql.connect(config);
        console.log('Conexión exitosa a SQL Server');
        return pool;
    } catch (err) {
        console.error('Error en la conexión a la base de datos:', err);
        throw err;
    }
}

async function enviarXML() {
    try {
        const pool = await conectarDB();

        let xmlContent = fs.readFileSync(xmlPath, 'utf-8');
        xmlContent = xmlContent.replace(/<\?xml[^>]*\?>\s*/, '');

        const result = await pool.request()
            .input('inArchivoXML', sql.NVarChar(sql.MAX), xmlContent)
            .output('outResultCode', sql.Int)
            .execute('CargarDatos');

        console.log('Código de resultado:', result.output.outResultCode);
        console.log('XML enviado y procesado en SQL Server.');
    } catch (err) {
        console.error('Error al enviar el XML:', err);
    } finally {
        await sql.close();
    }
}

enviarXML();
