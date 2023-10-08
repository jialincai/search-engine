const inputField = document.querySelector('#input-field');
const autocompleteResults = document.querySelector('#autocomplete-results');

const searchForm = document.querySelector('#search-form');

let autocompleteListItems = []; // Keep track of list items for keyboard navigation
let selectedIndex = -1; // Keep track of the selected item for keyboard navigation

// Attach the fetchAutocompleteResults function to the input field's input event
inputField.addEventListener('input', fetchAutocompleteResults);

searchForm.addEventListener('submit', function () {
    // Hide the autocomplete results
    autocompleteResults.style.display = 'none';
});

// Hide autocomplete results when user clicks outside it.
document.addEventListener('click', (event) => {
    if (!autocompleteResults.contains(event.target)) {
        autocompleteResults.style.display = 'none';
    }
});

// Function to fetch autocomplete results
function fetchAutocompleteResults() {
    const userInput = inputField.value.trim();

    // Make an API request to fetch the autocomplete results
    fetch(`/fetchAutocompleteResults?${new URLSearchParams({ userInput: userInput.toLowerCase() })}`)
        .then(response => { return response.json(); })
        .then(data => {
            // Hide the autocomplete results if there are no results
            if (data.length === 0) {
                autocompleteResults.style.display = 'none';
                return;
            }

            // Clear the previous autocomplete results
            autocompleteResults.innerHTML = '';
            autocompleteListItems = [];

            // Create a list of autocomplete suggestions
            const autocompleteList = document.createElement('ul');
            autocompleteList.classList.add('autocomplete-list');
            autocompleteResults.appendChild(autocompleteList);

            // Add each autocomplete suggestion as a list item
            data.forEach((suggestion, index) => {
                const listItem = document.createElement('li');
                listItem.textContent = suggestion;
                autocompleteList.appendChild(listItem);
                autocompleteListItems.push(listItem);

                // Handle mouse events
                listItem.addEventListener('mousedown', (event) => {
                    event.preventDefault(); // prevent input field from losing focus
                    selectAutocompleteItem(suggestion);
                });

                // Handle keyboard events
                listItem.addEventListener('mouseenter', (event) => {
                    selectedIndex = index;
                    highlightSelectedItem();
                });

                listItem.addEventListener('mouseleave', (event) => {
                    selectedIndex = -1;
                    highlightSelectedItem();
                });
            });

            // Show the autocomplete results
            autocompleteResults.style.display = 'block';
        })
        .catch(error => {
            console.error('Error fetching autocomplete results:', error);
        });
}

// Handle keyboard events for input field
inputField.addEventListener('keydown', (event) => {
    switch (event.keyCode) {
        case 38: // Up arrow key
            event.preventDefault();
            selectedIndex = selectedIndex > 0 ? selectedIndex - 1 : autocompleteListItems.length - 1;
            highlightSelectedItem();
            break;
        case 40: // Down arrow key
            event.preventDefault();
            selectedIndex = selectedIndex < autocompleteListItems.length - 1 ? selectedIndex + 1 : 0;
            highlightSelectedItem();
            break;
        case 13: // Enter key
            if (selectedIndex > -1) {
                event.preventDefault();
                const suggestion = autocompleteListItems[selectedIndex].textContent;
                selectAutocompleteItem(suggestion);
            }
            break;
    }
});

// Helper function to select an autocomplete item and update the input field
function selectAutocompleteItem(suggestion) {
    let modifiedValue = inputField.value.split(' ').slice(0, -1).join(' ');
    if (modifiedValue === '') {
        modifiedValue = suggestion;
    } else {
        modifiedValue = modifiedValue + ' ' + suggestion;
    }
    inputField.value = modifiedValue;
    autocompleteResults.innerHTML = '';
    autocompleteResults.style.display = 'none';
    selectedIndex = -1;
}

// Helper function to highlight the currently selected item for keyboard navigation
function highlightSelectedItem() {
    autocompleteListItems.forEach((item, index) => {
        if (index === selectedIndex) {
            item.classList.add('selected');
        } else {
            item.classList.remove('selected');
        }
    });
}