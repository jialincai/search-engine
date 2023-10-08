let results = []; // Array to store all search results
let resultsDisplayed = 0; // Number of search results currently displayed on the page
const resultsPerPage = 10; // Number of search results to display per page
const resultsContainer = document.querySelector('#search-results');
// Calculate the index of the last search result to display
let lastIndex = -1;

// Attach the fetchResults function to the form's submit event
searchForm.addEventListener('submit', fetchResults);
window.addEventListener('load', fetchAfterRedirect);

/**
 * Function to fetch all search results
 */
function fetchResults() {
    event.preventDefault(); // Prevent form from submitting normally

    const inputField = document.querySelector('#input-field');
    const userInput = inputField.value;

    // Erase previous results
    results = [];
    resultsContainer.innerHTML = '';
    resultsDisplayed = 0;

    // Fetch and display search results
    const requestOptions = {
        mode: "no-cors"
    }
    fetch(`/fetchResults?${new URLSearchParams({ userInput: userInput })}`, requestOptions)
        .then(response => { return response.json(); })
        .then(data => {
            // Parse the JSON response into an array of search results
            results = data.results;
            lastIndex = Math.min(resultsDisplayed + resultsPerPage, results.length);

            // Display the initial search results that fit on the page
            displaySearchResults();

            // Attach an event listener to the window for the scroll event
            window.addEventListener('scroll', checkScroll);
        })
        .catch(error => {
            console.error('Error fetching search results:', error);
        });
}

// Function to check if the user has scrolled to the bottom of the page
function checkScroll() {
    const scrollPosition = window.innerHeight + window.scrollY;
    const bodyHeight = document.body.offsetHeight;
    if (scrollPosition >= bodyHeight) {
        displaySearchResults();
    }
}

/**
 * Function to display search results based on how far the user has scrolled
 */
function displaySearchResults() {
    lastIndex = Math.min(resultsDisplayed + resultsPerPage, results.length);

    // Display the search results that fit on the page
    for (let i = resultsDisplayed; i < lastIndex; i++) {
        // Create a container element for the search result
        const resultElement = createResultElement(results[i]);
        resultsContainer.appendChild(resultElement);
    }

    // Update the variable tracking the number of search results displayed on the page
    resultsDisplayed = lastIndex;

    // Remove the scroll event listener if all search results have been displayed
    if (resultsDisplayed >= results.length) {
        window.removeEventListener('scroll', displaySearchResults);
    }
}

/**
 * Function to create an HTML element for a given search result
 * @param {object} result - An object representing a search result
 * @return {HTMLElement} A container element for the search result
 */
function createResultElement(result) {
    // Create a container element for the search result
    const resultElement = document.createElement('div');
    resultElement.classList.add('search-result');

    // Create a link element for the search result title
    const titleLink = document.createElement('a');
    titleLink.href = result.url;
    titleLink.textContent = result.title;
    resultElement.appendChild(titleLink);

    // Create a paragraph element for the url
    const urlElement = document.createElement('p');
    urlElement.classList.add("url");
    urlElement.textContent = result.url;
    resultElement.appendChild(urlElement);

    // Create a paragraph element for the search result description
    const descriptionElement = document.createElement('p');
    descriptionElement.textContent = result.description;
    descriptionElement.classList.add("description");
    resultElement.appendChild(descriptionElement);

    return resultElement;
}

function fetchAfterRedirect() {
    event.preventDefault();

    // Get the URL query parameters
    var queryParams = new URLSearchParams(window.location.search);

    if (queryParams.has("userInput")) {
        var input1 = document.querySelector('#input-field');
        input1.value = queryParams.get("userInput");
        fetchResults();
    }
}
