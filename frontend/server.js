import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = 3000;
const PYTHON_API_URL = 'http://127.0.0.1:8000/chat';

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// API Route to proxy requests to the Python Agent
app.post('/api/ask', async (req, res) => {
    try {
        const { message } = req.body;

        if (!message) {
            return res.status(400).json({ error: "Message is required." });
        }

        // Call the Python FastAPI service
        const pythonResponse = await fetch(PYTHON_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message })
        });

        if (!pythonResponse.ok) {
            throw new Error(`Python API responded with status: ${pythonResponse.status}`);
        }

        const data = await pythonResponse.json();

        // Return the agent's response to the frontend
        res.json({ reply: data.response, mql: data.mql });

    } catch (error) {
        console.error("Error communicating with Agent API:", error);
        res.status(500).json({ error: "Failed to process the request." });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Node.js Chat App running on http://localhost:${PORT}`);
});