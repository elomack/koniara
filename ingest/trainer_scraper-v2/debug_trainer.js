// ingest/trainer_scraper-v2/debug_trainer.js
import axios from 'axios';

async function debugTrainer(id) {
  try {
    const { data } = await axios.get(
      `https://homas.pkwk.org/homas/race/search/trainer/${id}`
    );
    const t = data;
    console.log('ALFA3:', JSON.stringify(t.licenceCountry?.alfa3, null, 2));
  } catch (err) {
    console.error('Error fetching trainer', id, err.message);
  }
}

const trainerId = process.argv[2] || '10';
debugTrainer(trainerId);
