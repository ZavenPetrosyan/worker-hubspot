const disallowedValues = [
  '[not provided]',
  'placeholder',
  '[[unknown]]',
  'not set',
  'not provided',
  'unknown',
  'undefined',
  'n/a'
];

const filterNullValuesFromObject = object =>
  Object
    .fromEntries(
      Object
        .entries(object)
        .filter(([_, v]) =>
          v !== null &&
          v !== '' &&
          typeof v !== 'undefined' &&
          (typeof v !== 'string' || !disallowedValues.includes(v.toLowerCase()) || !v.toLowerCase().includes('!$record'))));

const normalizePropertyName = key => key.toLowerCase().replace(/__c$/, '').replace(/^_+|_+$/g, '').replace(/_+/g, '_');

const goal = async (actions) => {
  console.log("Writing to MongoDB:", JSON.stringify(actions, null, 2));
  if (!actions.length) return;
  try {
    await mongoose.connection.db.collection("actions").insertMany(actions);
    console.log("Successfully inserted actions into MongoDB");
  } catch (error) {
    console.error("Error inserting actions into MongoDB:", error);
  }
};

module.exports = {
  filterNullValuesFromObject,
  normalizePropertyName,
  goal
};
