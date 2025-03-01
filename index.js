require('dotenv').config();

const axios = require('axios');
const AWS = require('aws-sdk');
const mongoose = require('mongoose');
const RestaurantMenu = require('./models/Menu');

const BUCKET_NAME = 'gobbl-restaurant-images-bucket';
const REGION = 'ap-south-1';
AWS.config.update({
  region: REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});
const s3 = new AWS.S3();

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  console.error("MONGO_URI is not set in the environment.");
  process.exit(1);
}
const DB_NAME = 'agentic';
const COLLECTION_NAME = 'restaurantmenus';

mongoose
  .connect(process.env.MONGODB_URL, {
    user: process.env.MONGODB_USER,
    pass: process.env.MONGODB_PASS,
  }).then(console.log("connnted"))
  .catch((err) => console.log(err));
/**
 * Downloads an image from the given URL and uploads it to S3.
 * The file is stored under a folder named with the restaurantId,
 * with a filename: `${restaurantId}/${restaurantId}-${itemId}.jpg`
 *
 * @param {number|string} restaurantId - The restaurant's ID.
 * @param {number|string} itemId - The menu item's ID.
 * @param {string} imageUrl - The original image URL.
 * @returns {Promise<string>} - The new S3 URL.
 */
async function uploadImageToS3(restaurantId, itemId, imageUrl) {
  try {
    const response = await axios.get(imageUrl, { responseType: 'arraybuffer' });
    const buffer = Buffer.from(response.data, 'binary');

    const key = `${restaurantId}/${restaurantId}-${itemId}.jpg`;

    await s3.putObject({
      Bucket: BUCKET_NAME,
      Key: key,
      Body: buffer,
      ContentType: 'image/jpeg'
    }).promise();

    return `https://${BUCKET_NAME}.s3.${REGION}.amazonaws.com/${key}`;
  } catch (error) {
    console.error(`Error uploading image for restaurantId ${restaurantId}, item ${itemId}:`, error.message);
    throw error;
  }
}

/**
 * Processes one restaurant document: updates each menu item's image URL.
 * If an image is missing or upload fails, it still sets the new URL in the given format.
 * @param {Object} doc - The restaurant menu document.
 * @param {Object} collection - The MongoDB collection.
 */
async function processDocument(doc) {
  const restaurantId = doc.restaurantId;
  if (!restaurantId || !Array.isArray(doc.items)) {
    console.warn(`Skipping document ${doc._id} due to missing restaurantId or items.`);
    return;
  }

  const updatedItems = [];
  for (const item of doc.items) {
    let newUrl;
    try {
      if (item.image) {
        newUrl = await uploadImageToS3(restaurantId, item.id, item.image);
      } else {
        newUrl = `https://gobbl-restaurant-images-bucket.s3.ap-south-1.amazonaws.com/landscape-placeholder-svgrepo-co.jpg`;
        console.log(`No original image for restaurantId ${restaurantId}, item ${item.id}. Setting URL to ${newUrl}`);
      }
    } catch (err) {
      console.error(`Failed to update image for restaurantId ${restaurantId}, item ${item.id}:`, err.message);
      newUrl = `https://gobbl-restaurant-images-bucket.s3.ap-south-1.amazonaws.com/landscape-placeholder-svgrepo-co.jpg`;
    }
    updatedItems.push({ ...item, image: newUrl });
  }

  await RestaurantMenu.findOneAndUpdate({ _id: doc._id }, { $set: { items: updatedItems } });
  console.log(`Updated document for restaurantId ${restaurantId}`);
}

async function updateRestaurantMenusImages() {
  try {
    const collection = RestaurantMenu.aggregate([
      {
        $skip: 101
      },
      {
        $project: {
          _id: 1,
          restaurantId: 1,
          items: 1
        }
      }
    ]);
    console.log("fetched from db");
    let count = 101;
    for (const doc of collection) {
      try {
        await processDocument(doc);
      } catch (error) {
        console.error(`Error processing document ${doc._id}:`, error.message);
      }
      count++;
      if (count % 100 === 0) {
        console.log(`Processed ${count} documents so far...`);
      }
    }
    console.log("Finished processing all documents.");
  } catch (error) {
    console.error("Error updating restaurant menus images:", error.message);
  } finally {
    mongoose.connection.close();
    process.exit(0);
  }
}

updateRestaurantMenusImages();

