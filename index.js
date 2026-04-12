require('dotenv').config();

const {
  Client,
  GatewayIntentBits,
  Events,
  SlashCommandBuilder,
  EmbedBuilder
} = require('discord.js');
const { google } = require('googleapis');

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const GUILD_ID = process.env.GUILD_ID;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const PILOTS_SHEET_NAME = process.env.PILOTS_SHEET_NAME || 'Pilots';
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY
  ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n')
  : undefined;

const BASE_ROLES = JSON.parse(process.env.BASE_ROLES_JSON || '{}');

const FLIGHTS_SHEET_NAME = 'Flight reports';
const JUMPSEAT_SHEET_NAME = 'Jumpseat';
const ROUTES_SHEET_NAME = 'Routes';

const DAILY_SUGGESTIONS_CHANNEL_ID = '1492448304460599406';
const PILOTS_ROLE_ID = '1492451469876527164';

const DAILY_SUGGESTION_HOUR_UTC = 11;
const DAILY_RESULT_HOUR_UTC = 17;
const DAILY_TIEBREAK_RESULT_HOUR_UTC = 17;
const DAILY_TIEBREAK_RESULT_MINUTE_UTC = 30;

if (
  !DISCORD_TOKEN ||
  !GUILD_ID ||
  !SPREADSHEET_ID ||
  !GOOGLE_SERVICE_ACCOUNT_EMAIL ||
  !GOOGLE_PRIVATE_KEY
) {
  console.error('Missing required variables in .env');
  process.exit(1);
}

console.log(
  GOOGLE_PRIVATE_KEY && GOOGLE_PRIVATE_KEY.startsWith('-----BEGIN')
    ? 'KEY FORMAT OK'
    : 'KEY FORMAT INVALID'
);

console.log('EMAIL OK:', !!process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL);
console.log('KEY OK:', !!process.env.GOOGLE_PRIVATE_KEY);
console.log('KEY LENGTH:', (process.env.GOOGLE_PRIVATE_KEY || '').length);

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessageReactions,
    GatewayIntentBits.GuildMessages
  ]
});

let sheets;
let lastDailySuggestionDate = null;
let lastDailyResultDate = null;
let lastDailyTieBreakDate = null;
let lastDailyTieBreakFinalDate = null;

/* =========================
   GOOGLE SHEETS
========================= */

async function initGoogleSheets() {
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
      private_key: GOOGLE_PRIVATE_KEY,
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const authClient = await auth.getClient();

  sheets = google.sheets({
    version: 'v4',
    auth: authClient,
  });

  console.log('Google Sheets auth OK');
}

async function getSheetRows(sheetName, range = 'A:Z') {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!${range}`
  });

  return res.data.values || [];
}

/* =========================
   UTILS
========================= */

function nowString() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function norm(v) {
  return String(v || '').trim();
}

function pilotKeyFromUsername(username) {
  return norm(username).toLowerCase();
}

function getPilotKeyFromMember(member) {
  return pilotKeyFromUsername(member.user.username);
}

function getBaseFromRoles(member) {
  for (const roleId of member.roles.cache.keys()) {
    if (BASE_ROLES[roleId]) return BASE_ROLES[roleId];
  }
  return '';
}

function parseTimestamp(value) {
  const d = new Date(value);
  const t = d.getTime();
  return Number.isNaN(t) ? 0 : t;
}

function parseFlightMinutes(value) {
  const raw = norm(value);
  if (!raw) return 0;

  if (raw.includes(':')) {
    const [h, m] = raw.split(':').map(x => parseInt(x, 10) || 0);
    return h * 60 + m;
  }

  const n = parseFloat(raw.replace(',', '.'));
  if (Number.isNaN(n)) return 0;
  return Math.round(n * 60);
}

function formatMinutes(totalMinutes) {
  const h = Math.floor(totalMinutes / 60);
  const m = totalMinutes % 60;
  return `${h}h ${String(m).padStart(2, '0')}m`;
}

function parseLanding(value) {
  const n = parseInt(norm(value), 10);
  return Number.isNaN(n) ? null : n;
}

function isBetterLanding(candidate, currentBest) {
  if (candidate === null) return false;
  if (currentBest === null) return true;
  return Math.abs(candidate) < Math.abs(currentBest);
}

function getMedal(index) {
  if (index === 0) return '🥇';
  if (index === 1) return '🥈';
  if (index === 2) return '🥉';
  return `#${index + 1}`;
}

function getLandingDisplay(value) {
  return value !== null ? `${value} fpm` : 'N/A';
}

function getRankSuffix(rank) {
  const j = rank % 10;
  const k = rank % 100;
  if (j === 1 && k !== 11) return `${rank}st`;
  if (j === 2 && k !== 12) return `${rank}nd`;
  if (j === 3 && k !== 13) return `${rank}rd`;
  return `${rank}th`;
}

function pickRandom(items) {
  if (!items.length) return null;
  return items[Math.floor(Math.random() * items.length)];
}

function formatSuggestedRoute(route) {
  if (!route) return 'No route available';
  const notes = route.notes ? ` — ${route.notes}` : '';
  return `\`${route.origin} → ${route.destination}\`${notes}`;
}

function roleMention() {
  return `<@&${PILOTS_ROLE_ID}>`;
}

function getTodayUtcKey(date = new Date()) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/* =========================
   ROUTES / DAILY SUGGESTIONS
========================= */

async function getRoutesRows() {
  return getSheetRows(ROUTES_SHEET_NAME, 'A:D');
}

async function getSuggestedFlights() {
  const rows = await getRoutesRows();

  const routes = [];
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const origin = norm(row[0]).toUpperCase();
    const destination = norm(row[1]).toUpperCase();
    const category = norm(row[2]).toUpperCase();
    const notes = norm(row[3]);

    if (!origin || !destination || !category) continue;
    if (!['SHORT', 'MEDIUM', 'LONG'].includes(category)) continue;

    routes.push({ origin, destination, category, notes });
  }

  const shortRoutes = routes.filter(r => r.category === 'SHORT');
  const mediumRoutes = routes.filter(r => r.category === 'MEDIUM');
  const longRoutes = routes.filter(r => r.category === 'LONG');

  return {
    short: pickRandom(shortRoutes),
    medium: pickRandom(mediumRoutes),
    long: pickRandom(longRoutes),
  };
}

async function sendDailySuggestions() {
  const channel = await client.channels.fetch(DAILY_SUGGESTIONS_CHANNEL_ID).catch(() => null);

  if (!channel || !channel.isTextBased()) {
    console.error('Daily suggestions channel not found or is not text-based');
    return;
  }

  const suggestions = await getSuggestedFlights();

  const embed = new EmbedBuilder()
    .setTitle('✈️ Daily Flight Suggestions')
    .setDescription(
      'Good morning, pilots.\n\n' +
      'Today’s route board for **18:00Z** is now open. Below you will find one **Short Haul**, one **Medium Haul**, and one **Long Haul** option.\n\n' +
      'Vote for the route you want us to operate this afternoon.'
    )
    .addFields(
      {
        name: '1️⃣ Short Haul',
        value: formatSuggestedRoute(suggestions.short),
        inline: false
      },
      {
        name: '2️⃣ Medium Haul',
        value: formatSuggestedRoute(suggestions.medium),
        inline: false
      },
      {
        name: '3️⃣ Long Haul',
        value: formatSuggestedRoute(suggestions.long),
        inline: false
      }
    )
    .setColor(0x00b894)
    .setFooter({ text: 'Sky Center • Voting closes at 17:00Z' })
    .setTimestamp();

  const message = await channel.send({
    content: roleMention(),
    embeds: [embed]
  });

  await message.react('1️⃣');
  await message.react('2️⃣');
  await message.react('3️⃣');

  console.log('Daily flight suggestions sent');
}

async function getLatestBotMessageByTitleFragment(titleFragment) {
  const channel = await client.channels.fetch(DAILY_SUGGESTIONS_CHANNEL_ID).catch(() => null);
  if (!channel || !channel.isTextBased()) return null;

  const messages = await channel.messages.fetch({ limit: 30 });
  const todayKey = getTodayUtcKey();

  const candidates = messages.filter(msg => {
    if (msg.author.id !== client.user.id) return false;
    if (!msg.embeds.length) return false;

    const title = msg.embeds[0].title || '';
    if (!title.includes(titleFragment)) return false;

    const msgKey = getTodayUtcKey(msg.createdAt);
    return msgKey === todayKey;
  });

  return candidates.first() || null;
}

function getVotesForReaction(message, emojiName) {
  const reaction = message.reactions.cache.get(emojiName);
  return reaction ? Math.max(reaction.count - 1, 0) : 0;
}

function buildOptionListFromSuggestionEmbed(embed) {
  const fields = embed.fields || [];
  return [
    {
      key: '1',
      emoji: '1️⃣',
      category: 'Short Haul',
      route: fields[0]?.value || 'No route available',
    },
    {
      key: '2',
      emoji: '2️⃣',
      category: 'Medium Haul',
      route: fields[1]?.value || 'No route available',
    },
    {
      key: '3',
      emoji: '3️⃣',
      category: 'Long Haul',
      route: fields[2]?.value || 'No route available',
    }
  ];
}

function buildSortedVoteOptions(options, voteMap) {
  return options
    .map(opt => ({
      ...opt,
      votes: voteMap[opt.emoji] || 0
    }))
    .sort((a, b) => b.votes - a.votes);
}

async function sendDailyResultAnnouncement() {
  const channel = await client.channels.fetch(DAILY_SUGGESTIONS_CHANNEL_ID).catch(() => null);

  if (!channel || !channel.isTextBased()) {
    console.error('Result channel not found or is not text-based');
    return;
  }

  const suggestionMessage = await getLatestBotMessageByTitleFragment('Daily Flight Suggestions');

  if (!suggestionMessage) {
    console.log('No daily suggestion message found for today');
    return;
  }

  const embed = suggestionMessage.embeds[0];
  const options = buildOptionListFromSuggestionEmbed(embed);

  const voteMap = {
    '1️⃣': getVotesForReaction(suggestionMessage, '1️⃣'),
    '2️⃣': getVotesForReaction(suggestionMessage, '2️⃣'),
    '3️⃣': getVotesForReaction(suggestionMessage, '3️⃣')
  };

  const ranked = buildSortedVoteOptions(options, voteMap);
  const winner = ranked[0];
  const second = ranked[1];

  const noVotes = winner.votes === 0;
  const tie = second && winner.votes === second.votes && winner.votes > 0;

  if (noVotes) {
    const resultEmbed = new EmbedBuilder()
      .setTitle('📢 Daily Operation Result')
      .setDescription(
        'Voting for today’s **18:00Z** operation has now closed.\n\n' +
        'No votes were cast for the available routes, so there is **no official flight selection** for today.\n\n' +
        '**See you all in the briefing room at 18:00Z.**'
      )
      .setColor(0x95a5a6)
      .addFields(
        {
          name: '1️⃣ Short Haul',
          value: `${options[0].route}\n**Votes:** ${voteMap['1️⃣']}`,
          inline: false
        },
        {
          name: '2️⃣ Medium Haul',
          value: `${options[1].route}\n**Votes:** ${voteMap['2️⃣']}`,
          inline: false
        },
        {
          name: '3️⃣ Long Haul',
          value: `${options[2].route}\n**Votes:** ${voteMap['3️⃣']}`,
          inline: false
        }
      )
      .setFooter({ text: 'Sky Center • Daily Result' })
      .setTimestamp();

    await channel.send({
      content: roleMention(),
      embeds: [resultEmbed]
    });

    console.log('Daily result sent with no votes');
    return;
  }

  if (tie) {
    const tiedOptions = ranked.filter(opt => opt.votes === winner.votes).slice(0, 2);

    const tieEmbed = new EmbedBuilder()
      .setTitle('⚖️ Tie-Break Poll')
      .setDescription(
        'The daily voting has ended in a **tie**.\n\n' +
        'A rapid tie-break vote is now open and will close at **17:30Z**.\n\n' +
        'Please vote again to decide today’s official operation for **18:00Z**.'
      )
      .addFields(
        {
          name: '1️⃣ Option A',
          value: tiedOptions[0].route,
          inline: false
        },
        {
          name: '2️⃣ Option B',
          value: tiedOptions[1].route,
          inline: false
        }
      )
      .setColor(0xf39c12)
      .setFooter({ text: 'Sky Center • Tie-break closes at 17:30Z' })
      .setTimestamp();

    const tieMessage = await channel.send({
      content: roleMention(),
      embeds: [tieEmbed]
    });

    await tieMessage.react('1️⃣');
    await tieMessage.react('2️⃣');

    console.log('Tie-break poll sent');
    return;
  }

  const resultEmbed = new EmbedBuilder()
    .setTitle('🏆 Official Flight of the Day')
    .setDescription(
      'Voting for today’s **18:00Z** operation has officially closed.\n\n' +
      'After today’s community vote, the selected route for this afternoon is:\n\n' +
      `✈️ **${winner.category}**\n${winner.route}\n\n` +
      `🗳 **Final Votes:** ${winner.votes}\n\n` +
      'All pilots are encouraged to prepare charts, briefing material and operational setup for the selected route.\n\n' +
      '**See you all in the briefing room at 18:00Z.**'
    )
    .addFields(
      {
        name: '1️⃣ Short Haul',
        value: `${options[0].route}\n**Votes:** ${voteMap['1️⃣']}`,
        inline: false
      },
      {
        name: '2️⃣ Medium Haul',
        value: `${options[1].route}\n**Votes:** ${voteMap['2️⃣']}`,
        inline: false
      },
      {
        name: '3️⃣ Long Haul',
        value: `${options[2].route}\n**Votes:** ${voteMap['3️⃣']}`,
        inline: false
      }
    )
    .setColor(0xe74c3c)
    .setFooter({ text: 'Sky Center • Official Daily Operation' })
    .setTimestamp();

  await channel.send({
    content: roleMention(),
    embeds: [resultEmbed]
  });

  console.log('Daily final winner announcement sent');
}

async function sendTieBreakFinalAnnouncement() {
  const channel = await client.channels.fetch(DAILY_SUGGESTIONS_CHANNEL_ID).catch(() => null);

  if (!channel || !channel.isTextBased()) {
    console.error('Tie-break result channel not found or is not text-based');
    return;
  }

  const tieMessage = await getLatestBotMessageByTitleFragment('Tie-Break Poll');

  if (!tieMessage) {
    console.log('No tie-break poll found for today');
    return;
  }

  const embed = tieMessage.embeds[0];
  const fields = embed.fields || [];

  const optionA = fields[0]?.value || 'No route available';
  const optionB = fields[1]?.value || 'No route available';

  const votesA = getVotesForReaction(tieMessage, '1️⃣');
  const votesB = getVotesForReaction(tieMessage, '2️⃣');

  if (votesA === 0 && votesB === 0) {
    const noVotesEmbed = new EmbedBuilder()
      .setTitle('📢 Tie-Break Result')
      .setDescription(
        'The tie-break vote has now closed.\n\n' +
        'No votes were cast during the tie-break, so both routes remain unresolved.\n\n' +
        'Operations staff may choose the final route manually.\n\n' +
        '**See you all in the briefing room at 18:00Z.**'
      )
      .addFields(
        {
          name: '1️⃣ Option A',
          value: `${optionA}\n**Votes:** ${votesA}`,
          inline: false
        },
        {
          name: '2️⃣ Option B',
          value: `${optionB}\n**Votes:** ${votesB}`,
          inline: false
        }
      )
      .setColor(0x95a5a6)
      .setFooter({ text: 'Sky Center • Tie-Break Result' })
      .setTimestamp();

    await channel.send({
      content: roleMention(),
      embeds: [noVotesEmbed]
    });

    console.log('Tie-break result sent with no votes');
    return;
  }

  if (votesA === votesB) {
    const doubleWinnerEmbed = new EmbedBuilder()
      .setTitle('⚖️ Tie-Break Result')
      .setDescription(
        'The tie-break vote has ended in **another tie**.\n\n' +
        'Both routes will stand as today’s final shortlisted operations:\n\n' +
        `**Option A**\n${optionA}\n\n` +
        `**Option B**\n${optionB}\n\n` +
        'Pilots may operate either of the two tied routes unless staff decides otherwise.\n\n' +
        '**See you all in the briefing room at 18:00Z.**'
      )
      .addFields(
        {
          name: '1️⃣ Option A',
          value: `Votes: **${votesA}**`,
          inline: true
        },
        {
          name: '2️⃣ Option B',
          value: `Votes: **${votesB}**`,
          inline: true
        }
      )
      .setColor(0xf1c40f)
      .setFooter({ text: 'Sky Center • Final Tie Outcome' })
      .setTimestamp();

    await channel.send({
      content: roleMention(),
      embeds: [doubleWinnerEmbed]
    });

    console.log('Tie-break final ended in a tie');
    return;
  }

  const winnerRoute = votesA > votesB ? optionA : optionB;
  const winnerLabel = votesA > votesB ? 'Option A' : 'Option B';
  const winnerVotes = Math.max(votesA, votesB);

  const resultEmbed = new EmbedBuilder()
    .setTitle('🏆 Official Flight of the Day')
    .setDescription(
      'The tie-break vote has now closed, and today’s official route for the **18:00Z** operation is:\n\n' +
      `✈️ **${winnerLabel}**\n${winnerRoute}\n\n` +
      `🗳 **Winning Votes:** ${winnerVotes}\n\n` +
      'Briefing and preparation should now proceed for the selected route.\n\n' +
      '**See you all in the briefing room at 18:00Z.**'
    )
    .addFields(
      {
        name: '1️⃣ Option A',
        value: `${optionA}\n**Votes:** ${votesA}`,
        inline: false
      },
      {
        name: '2️⃣ Option B',
        value: `${optionB}\n**Votes:** ${votesB}`,
        inline: false
      }
    )
    .setColor(0xe74c3c)
    .setFooter({ text: 'Sky Center • Tie-Break Winner' })
    .setTimestamp();

    await channel.send({
      content: roleMention(),
      embeds: [resultEmbed]
    });

  console.log('Tie-break final winner announcement sent');
}

function startDailySuggestionsScheduler() {
  setInterval(async () => {
    try {
      const now = new Date();
      const todayKey = getTodayUtcKey(now);
      const hour = now.getUTCHours();
      const minute = now.getUTCMinutes();

      if (
        hour === DAILY_SUGGESTION_HOUR_UTC &&
        minute === 0 &&
        lastDailySuggestionDate !== todayKey
      ) {
        await sendDailySuggestions();
        lastDailySuggestionDate = todayKey;
      }

      if (
        hour === DAILY_RESULT_HOUR_UTC &&
        minute === 0 &&
        lastDailyResultDate !== todayKey
      ) {
        await sendDailyResultAnnouncement();
        lastDailyResultDate = todayKey;
      }

      if (
        hour === DAILY_TIEBREAK_RESULT_HOUR_UTC &&
        minute === DAILY_TIEBREAK_RESULT_MINUTE_UTC &&
        lastDailyTieBreakFinalDate !== todayKey
      ) {
        await sendTieBreakFinalAnnouncement();
        lastDailyTieBreakFinalDate = todayKey;
      }
    } catch (err) {
      console.error('Daily scheduler error:', err.message);
    }
  }, 60 * 1000);
}

/* =========================
   PILOTS SYNC
========================= */

async function getAllPilotsRows() {
  return getSheetRows(PILOTS_SHEET_NAME, 'A:F');
}

async function findPilotRow(discordKey) {
  const rows = await getAllPilotsRows();

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const sheetDiscordId = pilotKeyFromUsername(row[0]);

    if (sheetDiscordId === discordKey) {
      return {
        rowNumber: i + 1,
        values: row
      };
    }
  }

  return null;
}

async function addPilotRow({ discordId, baseAirport, active, notes, inServer, lastSync }) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${PILOTS_SHEET_NAME}!A:F`,
    valueInputOption: 'USER_ENTERED',
    requestBody: {
      values: [[discordId, baseAirport, active, notes, inServer, lastSync]]
    }
  });
}

async function updatePilotRow(rowNumber, { discordId, baseAirport, active, notes, inServer, lastSync }) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${PILOTS_SHEET_NAME}!A${rowNumber}:F${rowNumber}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: {
      values: [[discordId, baseAirport, active, notes, inServer, lastSync]]
    }
  });
}

async function upsertPilotFromMember(member) {
  const discordId = getPilotKeyFromMember(member);
  if (!discordId) return;

  const baseAirport = getBaseFromRoles(member);
  const lastSync = nowString();

  const existing = await findPilotRow(discordId);

  const payload = {
    discordId,
    baseAirport,
    active: 'YES',
    notes: 'Bot sync',
    inServer: 'YES',
    lastSync
  };

  if (existing) {
    const oldBase = norm(existing.values[1]).toUpperCase();
    if (!payload.baseAirport && oldBase) payload.baseAirport = oldBase;

    const oldNotes = norm(existing.values[3]);
    if (oldNotes && oldNotes !== 'Bot sync') payload.notes = oldNotes;

    await updatePilotRow(existing.rowNumber, payload);
    console.log(`Updated pilot: ${discordId} -> ${payload.baseAirport || '(no base)'}`);
  } else {
    await addPilotRow(payload);
    console.log(`Added pilot: ${discordId} -> ${payload.baseAirport || '(no base)'}`);
  }
}

async function markPilotOutOfServer(member) {
  const discordId = getPilotKeyFromMember(member);
  if (!discordId) return;

  const existing = await findPilotRow(discordId);
  if (!existing) return;

  const old = existing.values;

  await updatePilotRow(existing.rowNumber, {
    discordId,
    baseAirport: norm(old[1]).toUpperCase(),
    active: norm(old[2]).toUpperCase() || 'YES',
    notes: norm(old[3]) || 'Bot sync',
    inServer: 'NO',
    lastSync: nowString()
  });

  console.log(`Marked as out of server: ${discordId}`);
}

async function initialSync() {
  console.log('Starting initial sync...');

  const guild = await client.guilds.fetch(GUILD_ID);
  const members = await guild.members.fetch();

  for (const [, member] of members) {
    if (member.user.bot) continue;
    await upsertPilotFromMember(member);
  }

  console.log('Initial sync completed');
}

/* =========================
   STATS / RANKINGS
========================= */

async function getPilotsMap() {
  const rows = await getAllPilotsRows();
  const map = new Map();

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const key = pilotKeyFromUsername(row[0]);
    if (!key) continue;

    map.set(key, {
      discordId: key,
      base: norm(row[1]).toUpperCase(),
      active: norm(row[2]).toUpperCase(),
      notes: norm(row[3]),
      inServer: norm(row[4]).toUpperCase(),
      lastSync: norm(row[5]),
    });
  }

  return map;
}

async function getFlightStatsMap() {
  const rows = await getSheetRows(FLIGHTS_SHEET_NAME, 'A:N');
  const stats = new Map();

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];

    const timestamp = parseTimestamp(row[0]);
    const discordId = pilotKeyFromUsername(row[1]);
    const arr = norm(row[4]).toUpperCase();
    const flightTime = norm(row[6]);
    const landing = parseLanding(row[7]);
    const status = norm(row[10]).toUpperCase();

    if (!discordId || status !== 'CONFIRMED') continue;

    if (!stats.has(discordId)) {
      stats.set(discordId, {
        discordId,
        totalMinutes: 0,
        flights: 0,
        bestLanding: null,
        lastFlightTime: 0,
        lastFlightArrival: ''
      });
    }

    const s = stats.get(discordId);
    s.totalMinutes += parseFlightMinutes(flightTime);
    s.flights += 1;

    if (isBetterLanding(landing, s.bestLanding)) {
      s.bestLanding = landing;
    }

    if (timestamp >= s.lastFlightTime) {
      s.lastFlightTime = timestamp;
      s.lastFlightArrival = arr;
    }
  }

  return stats;
}

async function getJumpseatLastPositionMap() {
  const rows = await getSheetRows(JUMPSEAT_SHEET_NAME, 'A:H');
  const map = new Map();

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];

    const timestamp = parseTimestamp(row[0]);
    const discordId = pilotKeyFromUsername(row[1]);
    const arr = norm(row[3]).toUpperCase();
    const status = norm(row[4]).toUpperCase();

    if (!discordId || status !== 'CONFIRMED') continue;

    if (!map.has(discordId)) {
      map.set(discordId, {
        lastJumpseatTime: 0,
        lastJumpseatArrival: ''
      });
    }

    const s = map.get(discordId);
    if (timestamp >= s.lastJumpseatTime) {
      s.lastJumpseatTime = timestamp;
      s.lastJumpseatArrival = arr;
    }
  }

  return map;
}

async function buildPilotStats() {
  const pilotsMap = await getPilotsMap();
  const flightsMap = await getFlightStatsMap();
  const jumpseatMap = await getJumpseatLastPositionMap();

  const allKeys = new Set([
    ...pilotsMap.keys(),
    ...flightsMap.keys(),
    ...jumpseatMap.keys()
  ]);

  const result = [];

  for (const key of allKeys) {
    const pilot = pilotsMap.get(key) || {};
    const flight = flightsMap.get(key) || {
      totalMinutes: 0,
      flights: 0,
      bestLanding: null,
      lastFlightTime: 0,
      lastFlightArrival: ''
    };
    const jump = jumpseatMap.get(key) || {
      lastJumpseatTime: 0,
      lastJumpseatArrival: ''
    };

    let lastPosition = pilot.base || '';
    let lastPositionTime = 0;

    if (flight.lastFlightTime >= lastPositionTime && flight.lastFlightArrival) {
      lastPosition = flight.lastFlightArrival;
      lastPositionTime = flight.lastFlightTime;
    }

    if (jump.lastJumpseatTime >= lastPositionTime && jump.lastJumpseatArrival) {
      lastPosition = jump.lastJumpseatArrival;
      lastPositionTime = jump.lastJumpseatTime;
    }

    result.push({
      discordId: key,
      base: pilot.base || '',
      active: pilot.active || '',
      inServer: pilot.inServer || '',
      totalMinutes: flight.totalMinutes || 0,
      flights: flight.flights || 0,
      bestLanding: flight.bestLanding,
      lastPosition
    });
  }

  return result;
}

async function getStatsForPilot(discordId) {
  const all = await buildPilotStats();
  return all.find(p => p.discordId === pilotKeyFromUsername(discordId)) || null;
}

async function getRankingByHours() {
  const all = await buildPilotStats();

  return all
    .filter(p => p.flights > 0)
    .sort((a, b) => {
      if (b.totalMinutes !== a.totalMinutes) return b.totalMinutes - a.totalMinutes;
      return b.flights - a.flights;
    });
}

async function getRankingByBestLandings() {
  const all = await buildPilotStats();

  return all
    .filter(p => p.bestLanding !== null)
    .sort((a, b) => Math.abs(a.bestLanding) - Math.abs(b.bestLanding));
}

function resolveTargetDiscordId(interaction) {
  const user = interaction.options.getUser('user');
  return user ? pilotKeyFromUsername(user.username) : pilotKeyFromUsername(interaction.user.username);
}

/* =========================
   SLASH COMMANDS
========================= */

async function registerCommands() {
  const commands = [
    new SlashCommandBuilder()
      .setName('ranking')
      .setDescription('Show the top 5 pilots by total flight hours'),

    new SlashCommandBuilder()
      .setName('position')
      .setDescription('Show a pilot ranking position by total hours')
      .addUserOption(option =>
        option
          .setName('user')
          .setDescription('The user to inspect')
          .setRequired(false)
      ),

    new SlashCommandBuilder()
      .setName('stats')
      .setDescription('Show a pilot statistics card')
      .addUserOption(option =>
        option
          .setName('user')
          .setDescription('The user to inspect')
          .setRequired(false)
      ),

    new SlashCommandBuilder()
      .setName('toplandings')
      .setDescription('Show the top 5 best landings'),

    new SlashCommandBuilder()
      .setName('suggestflights')
      .setDescription('Show 3 suggested flights: short, medium and long haul')
  ].map(command => command.toJSON());

  const guild = await client.guilds.fetch(GUILD_ID);
  await guild.commands.set(commands);
  console.log('Slash commands registered');
}

client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  try {
    if (interaction.commandName === 'ranking') {
      const ranking = await getRankingByHours();
      const top5 = ranking.slice(0, 5);

      if (!top5.length) {
        await interaction.reply({ content: 'No confirmed flights found yet.', ephemeral: true });
        return;
      }

      const lines = top5.map((p, i) => {
        const medal = getMedal(i);
        return `${medal} **${p.discordId}**\n┗ Hours: **${formatMinutes(p.totalMinutes)}** • Flights: **${p.flights}** • Best LDG: **${getLandingDisplay(p.bestLanding)}**`;
      });

      const embed = new EmbedBuilder()
        .setTitle('🏆 Sky Center Ranking')
        .setDescription(lines.join('\n\n'))
        .setColor(0x3498db)
        .setFooter({ text: 'Top 5 by total hours' })
        .setTimestamp();

      await interaction.reply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'position') {
      const targetId = resolveTargetDiscordId(interaction);
      const ranking = await getRankingByHours();
      const index = ranking.findIndex(p => p.discordId === targetId);

      if (index === -1) {
        await interaction.reply({ content: `No confirmed flights found for **${targetId}**.`, ephemeral: true });
        return;
      }

      const pilot = ranking[index];
      const rank = index + 1;

      const embed = new EmbedBuilder()
        .setTitle('📍 Ranking Position')
        .setColor(0x2ecc71)
        .addFields(
          { name: 'Pilot', value: `**${pilot.discordId}**`, inline: true },
          { name: 'Position', value: `**${getRankSuffix(rank)}**`, inline: true },
          { name: 'Total Hours', value: `**${formatMinutes(pilot.totalMinutes)}**`, inline: true },
          { name: 'Total Flights', value: `**${pilot.flights}**`, inline: true }
        )
        .setTimestamp();

      await interaction.reply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'stats') {
      const targetId = resolveTargetDiscordId(interaction);
      const stats = await getStatsForPilot(targetId);

      if (!stats) {
        await interaction.reply({ content: `No data found for **${targetId}**.`, ephemeral: true });
        return;
      }

      const embed = new EmbedBuilder()
        .setTitle('📊 Pilot Statistics')
        .setColor(0xf1c40f)
        .addFields(
          { name: 'Pilot', value: `**${stats.discordId}**`, inline: true },
          { name: 'Base', value: `**${stats.base || 'N/A'}**`, inline: true },
          { name: 'Last Position', value: `**${stats.lastPosition || 'N/A'}**`, inline: true },
          { name: 'Total Hours', value: `**${formatMinutes(stats.totalMinutes)}**`, inline: true },
          { name: 'Total Flights', value: `**${stats.flights}**`, inline: true },
          { name: 'Best Landing', value: `**${getLandingDisplay(stats.bestLanding)}**`, inline: true }
        )
        .setTimestamp();

      await interaction.reply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'toplandings') {
      const ranking = await getRankingByBestLandings();
      const top5 = ranking.slice(0, 5);

      if (!top5.length) {
        await interaction.reply({ content: 'No confirmed landings found yet.', ephemeral: true });
        return;
      }

      const lines = top5.map((p, i) => {
        const medal = getMedal(i);
        return `${medal} **${p.discordId}**\n┗ Best Landing: **${getLandingDisplay(p.bestLanding)}** • Flights: **${p.flights}**`;
      });

      const embed = new EmbedBuilder()
        .setTitle('🛬 Top Landings')
        .setDescription(lines.join('\n\n'))
        .setColor(0x9b59b6)
        .setFooter({ text: 'Top 5 best landing rates' })
        .setTimestamp();

      await interaction.reply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'suggestflights') {
      const suggestions = await getSuggestedFlights();

      const embed = new EmbedBuilder()
        .setTitle('✈️ Suggested Flights')
        .setDescription('Here are 3 route suggestions for today:')
        .addFields(
          {
            name: 'Short Haul',
            value: formatSuggestedRoute(suggestions.short),
            inline: false
          },
          {
            name: 'Medium Haul',
            value: formatSuggestedRoute(suggestions.medium),
            inline: false
          },
          {
            name: 'Long Haul',
            value: formatSuggestedRoute(suggestions.long),
            inline: false
          }
        )
        .setColor(0x0984e3)
        .setTimestamp();

      await interaction.reply({ embeds: [embed] });
      return;
    }
  } catch (err) {
    console.error('Interaction error:', err);
    const content = 'An error occurred while executing this command.';
    if (interaction.replied || interaction.deferred) {
      await interaction.followUp({ content, ephemeral: true });
    } else {
      await interaction.reply({ content, ephemeral: true });
    }
  }
});

/* =========================
   READY / EVENTS
========================= */

client.once(Events.ClientReady, async (readyClient) => {
  console.log(`Bot online as ${readyClient.user.tag}`);

  try {
    await initGoogleSheets();
    await initialSync();
    await registerCommands();
    startDailySuggestionsScheduler();
  } catch (err) {
    console.error('Initial sync failed:', err.message);
  }
});

client.on(Events.GuildMemberAdd, async (member) => {
  try {
    if (member.user.bot) return;
    await upsertPilotFromMember(member);
  } catch (err) {
    console.error('GuildMemberAdd error:', err.message);
  }
});

client.on(Events.GuildMemberUpdate, async (_oldMember, newMember) => {
  try {
    if (newMember.user.bot) return;
    await upsertPilotFromMember(newMember);
  } catch (err) {
    console.error('GuildMemberUpdate error:', err.message);
  }
});

client.on(Events.GuildMemberRemove, async (member) => {
  try {
    if (member.user.bot) return;
    await markPilotOutOfServer(member);
  } catch (err) {
    console.error('GuildMemberRemove error:', err.message);
  }
});

client.login(DISCORD_TOKEN);