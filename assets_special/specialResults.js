const specialResultsContainer = document.querySelector('#special-results');

// Attach the fetchResults function to the form's submit event
searchForm.addEventListener('submit', fetchSpecialResults);

/**
 * Function to fetch all search results
 */
function fetchSpecialResults() {
    event.preventDefault(); // Prevent form from submitting normally

    const inputField = document.querySelector('#input-field');
    const userInput = inputField.value;

    // Remove any existing weather data
    removeWeatherData();

    // Fetch and display weather data if the user entered a weather search query
    if (userInput.toLowerCase().startsWith('weather ')) {
        const location = getLocationFromQuery(userInput);
        fetchAndDisplayWeatherData(location);
    }
}

/**
 * Function to remove any existing weather data from the page
 */
function removeWeatherData() {
    const weatherElement = document.querySelector('.weather-data');
    if (weatherElement) {
        weatherElement.remove();
    }
}

/**
 * Function to get the location from a weather search query
 * @param {string} query - The user's search query
 * @return {string} The location from the search query
 */
function getLocationFromQuery(query) {
    const location = query.slice('weather'.length).trim();
    return location;
}

/**
 * Function to fetch and display weather data for a given location
 * @param {string} location - The location for which to fetch weather data
 */
function fetchAndDisplayWeatherData(location) {
    fetchLocationData(location)
        .then(locationData => {
            location = locationData.name;
            // Call the fetchWeatherData function to get the weather data
            return fetchWeatherData(locationData.latitude, locationData.longitude);
        })
        .then(weatherData => {
            // Create a container element for the weather data
            const weatherElement = document.createElement('div');
            weatherElement.classList.add('weather-data');
            specialResultsContainer.insertBefore(weatherElement, specialResultsContainer.firstChild);

            // Display the weather data
            const temperature = weatherData.hourly["temperature_2m"][18];
            const weatherDescription = weatherData.hourly["weathercode"][12];
            weatherElement.textContent = `The current weather in ${location} is ${temperature} degrees Fahrenheit and ${weatherDescription}.`;
        })
        .catch(error => {
            console.error('Error fetching weather data:', error);
        });
}

/**
 * Function to fetch location data for a given location
 * @param {string} location - A city name or zipcode for the desired location
 * @return {Promise} A promise that resolves with the location data for the given location
 */
function fetchLocationData(location) {
    // Make an API request to fetch the location data for the given location
    return fetch(`https://geocoding-api.open-meteo.com/v1/search?name=${location}&count=1&language=en&format=json`)
        .then(response => { return response.json(); })
        .then(data => { return data.results[0]; })
        .catch(error => {
            console.error('Error fetching location data:', error);
        });
}

/**
 * Function to fetch weather data for a given latitude and longitude
 * @param {string} latitude - The latitude of the desired location
 * @param {string} longitude - The longitude of the desired location
 * @return {Promise} A promise that resolves with the weather data for the given location
 */
function fetchWeatherData(latitude, longitude) {
    // Make an API request to fetch weather data for the location
    return fetch(`https://api.open-meteo.com/v1/forecast?latitude=${latitude}&longitude=${longitude}&&hourly=precipitation_probability,weathercode,temperature_2m&temperature_unit=fahrenheit&forecast_days=1&timezone=America%2FNew_York`)
        .then(response => { return response.json(); })
        .then(data => { return data; })
        .catch(error => {
            console.error('Error fetching weather data:', error);
        });
}
