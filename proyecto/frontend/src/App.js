import { Routes, Route, Navigate } from 'react-router-dom';
import './index.css';
import './App.css';
import Login from './login/login';

export default function App() {
  console.log("test");
  return(
    <div>

      {/* Definición de rutas */}
      <Routes>
        {/* Ruta principal de Login */}
        <Route path={'/login'} element={<Login/>} />


        {/* Redirige "/" a "/login" */}
        <Route path="/" element={<Navigate to={'/login'} replace />} />
        {/* <Route path="/empleados" element={<EmployeeList />} />
        <Route path="/empleados/insertar" element={<InsertarEmpleado />} />
        <Route path="/empleados/movimientos" element={<MovimientosEmpleado />} />
        <Route path="/empleados/actualizar" element={<ActualizarEmpleado />} />
        <Route path="/empleados/movimiento/incertar" element={<InsertarMovimiento/>}/> */}
        {/* Cualquier otra ruta, de nuevo a login */}
        <Route path="*" element={<Navigate to={'/login'} replace />} />
      </Routes>
    </div>
  );
}
