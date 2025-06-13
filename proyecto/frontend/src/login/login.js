import { useState } from 'react';
import axios from 'axios';
import './login.css';

function Login() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error,    setError]    = useState('');

  const handleSubmit = async e => {
    e.preventDefault();
    setError('');
    try {
      const { data } = await axios.post('/api/login', { username, password });
      localStorage.setItem('username', username);
      // onSuccess();
    } catch (err) {
      const { data } = err.response || {};
      const message = data?.message || 'Error de conexión al servidor';
      const fails   = data?.fails;

      setError(
        fails !== undefined
          ? `${message}  (Intentos fallidos en 20 min: ${fails})`
          : message
      );
    }
  };

  return (
    <div className="login-container">
      <form className="login-card" onSubmit={handleSubmit}>
        <h2 className="login-title">Iniciar Sesión</h2>
        {error && <div className="login-error">{error}</div>}

        <label>
          Usuario
          <input
            type="text"
            className="login-input"
            value={username}
            onChange={e => setUsername(e.target.value)}
            required
          />
        </label>

        <label>
          Contraseña
          <input
            type="password"
            className="login-input"
            value={password}
            onChange={e => setPassword(e.target.value)}
            required
          />
        </label>

        <button type="submit" className="login-button">Entrar</button>
      </form>
    </div>
  );
}

export default Login;