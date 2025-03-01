var mongoose = require("mongoose");

const AddOnItemSchema = new mongoose.Schema({
    name: { type: String, required: true },
    price: { type: Number, required: true },
});

const AddOnCategorySchema = new mongoose.Schema({
    categoryName: { type: String, required: true },
    minQuantity: { type: Number, required: true },
    maxQuantity: { type: Number, required: true },
    items: [AddOnItemSchema],
});

const ItemCustomisationSchema = new mongoose.Schema({
    categories: [AddOnCategorySchema],
});

const MenuItemSchema = new mongoose.Schema({
    id: { type: Number, required: true },
    name: { type: String, required: true },
    description: { type: String, default: '' },
    category: { type: String, default: '' },
    price: { type: Number, default: 0 },
    image: { type: String, default: '' },
    spicinessLevel: { type: Number, default: 0 },
    sweetnessLevel: { type: Number, default: 0 },
    dietaryPreference: [{ type: String }],
    healthinessScore: { type: Number, default: 0 },
    caffeineLevel: { type: String, default: 'None' },
    sufficientFor: { type: Number, default: 1 },
    available: { type: Boolean, default: true },
    isCustomisable: { type: Boolean, default: false },
    customisation: {
        type: ItemCustomisationSchema,
        default: { categories: [] },
    },
});

var RestaurantMenu = new mongoose.Schema({
    restaurantId: { type: Number, required: true, unique: true },
    restaurantName: { type: String, required: true },
    items: [MenuItemSchema],
    lastUpdated: { type: Date, default: Date.now },
}, { timestamps: true });

module.exports = mongoose.model("RestaurantMenu", RestaurantMenu);