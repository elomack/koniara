// ingest/trainer_scraper-v2/debug_trainer.js
import axios from 'axios';

async function debugTrainer(id) {
  try {
    const { data } = await axios.get(
      `https://homas.pkwk.org/homas/race/search/trainer/${id}`
    );
    console.log('FULL PAYLOAD:', JSON.stringify(data, null, 2));
  } catch (err) {
    console.error('Error fetching trainer', id, err.message);
  }
}

const trainerId = process.argv[2] || '10';
debugTrainer(trainerId);
