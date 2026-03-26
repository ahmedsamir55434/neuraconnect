# NeuraConnect

Real-time social platform with stories, posts, meetings, and AI features.

## Features

- **Authentication**: Login/Register with email verification
- **Social Feed**: Posts, likes, saves, shares
- **Stories**: Interactive stories with polls, questions, reactions
- **Meetings**: Video/audio calls with approval system, chat, reactions, recording
- **AI Integration**: Gemini-powered content generation and chat
- **Admin Panel**: User management, notifications, content moderation
- **Real-time**: WebSocket support for meetings and live interactions

## Quick Start

1. **Install dependencies**
   ```bash
   npm install
   ```

2. **Set up environment**
   ```bash
   cp .env.example .env
   # Edit .env and add your GEMINI_API_KEY
   ```

3. **Start the server**
   ```bash
   npm start
   ```

4. **Open your browser**
   Navigate to `http://localhost:4000`

## Environment Variables

- `GEMINI_API_KEY`: Required - Get from [Google AI Studio](https://makersuite.google.com/app/apikey)
- `GEMINI_MODEL`: Optional - Defaults to `gemini-2.0-flash`
- `PORT`: Optional - Defaults to `4000`
- `ADMIN_EMAILS`: Optional - Comma-separated emails for admin access

## Project Structure

```
├── public/                 # Frontend assets
│   ├── index.html         # Main HTML file
│   ├── app.js            # Frontend JavaScript
│   └── styles.css        # CSS styles
├── data/                  # Database storage (auto-created)
├── server.js             # Backend server
├── package.json          # Dependencies and scripts
├── .env.example          # Environment template
└── README.md             # This file
```

## Development

The project uses:
- **Backend**: Node.js with Express, WebSocket, bcryptjs
- **Frontend**: Vanilla JavaScript with modern ES6+ features
- **Database**: JSON file storage (auto-created in `data/` directory)
- **Real-time**: WebSocket for meetings and live features

## Meetings Feature

The meetings system supports:
- Video/audio calls with WebRTC
- Approval-based joining
- Real-time chat
- Reactions and raise hand
- Local recording
- Screen sharing
- Device selection

## License

MIT License
