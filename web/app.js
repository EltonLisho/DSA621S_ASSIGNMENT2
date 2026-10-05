const API = window.FOOD_API_BASE || '/api';
const money = amount => `N$ ${Number(amount).toFixed(2)}`;
const restaurantArt = {savanna: '🍲', coast: '🐟', garden: '🥬'};
const lifecycle = ['CREATED', 'CONFIRMED', 'PREPARING', 'READY', 'OUT_FOR_DELIVERY', 'DELIVERED'];

let restaurants = [];
let orders = [];
let cart = {};
let activeFilter = 'All';
let selectedRestaurant = null;

const grid = document.querySelector('#restaurant-grid');
const toastElement = document.querySelector('#toast');

function toast(message) {
    toastElement.textContent = message;
    toastElement.classList.add('show');
    setTimeout(() => toastElement.classList.remove('show'), 2600);
}

async function api(path, options = {}) {
    const response = await fetch(`${API}${path}`, {
        ...options,
        headers: {'Content-Type': 'application/json', ...(options.headers || {})}
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Something went wrong');
    return data;
}

async function loadRestaurants() {
    restaurants = await api('/restaurants');
    renderRestaurants();
}

function renderRestaurants() {
    const visible = restaurants.filter(restaurant =>
        activeFilter === 'All' || restaurant.cuisine.includes(activeFilter)
    );
    grid.innerHTML = visible.map(restaurant => `
        <article class="restaurant-card">
          <div class="restaurant-cover" style="background:${restaurant.color}1f">
            <span class="cover-stamp">NEIGHBOURHOOD FAVOURITE</span>
            <span class="cover-emoji">${restaurantArt[restaurant.id] || '🍽️'}</span>
          </div>
          <div class="restaurant-info">
            <div class="restaurant-title"><h3>${restaurant.name}</h3><span class="rating">★ ${restaurant.rating}</span></div>
            <div class="restaurant-meta">${restaurant.cuisine} · ${restaurant.eta}</div>
            <div class="restaurant-meta">${restaurant.isOpen ? 'Open' : 'Closed'} · ${restaurant.hours}</div>
            <div class="menu-list">${restaurant.menu.map(item => `
              <div class="menu-item">
                <div><strong>${item.name}</strong><small>${item.description}</small><small>${item.stock} in stock</small></div>
                <div class="item-side">
                  <span class="item-price">${money(item.price)}</span>
                  <button class="add-button" data-r="${restaurant.id}" data-i="${item.id}" ${item.stock < 1 || !restaurant.isOpen ? 'disabled' : ''}>
                    ${item.stock < 1 ? 'Sold out' : '+ Add'}
                  </button>
                </div>
              </div>`).join('')}</div>
          </div>
        </article>`).join('');

    grid.querySelectorAll('.add-button:not(:disabled)').forEach(button => {
        button.onclick = () => addToCart(button.dataset.r, button.dataset.i);
    });
}

function addToCart(restaurantId, itemId) {
    if (selectedRestaurant && selectedRestaurant !== restaurantId) {
        toast('Your bag is from another kitchen. Place that order first.');
        return;
    }
    selectedRestaurant = restaurantId;
    cart[itemId] = (cart[itemId] || 0) + 1;
    document.querySelector('#cart-count').textContent = Object.values(cart).reduce((sum, count) => sum + count, 0);
    toast('Added to your bag');
}

function cartDetails() {
    const restaurant = restaurants.find(item => item.id === selectedRestaurant);
    if (!restaurant) return {restaurant: null, items: [], total: 0};
    const items = Object.entries(cart).map(([id, quantity]) => ({
        id,
        quantity,
        item: restaurant.menu.find(menuItem => menuItem.id === id)
    }));
    return {
        restaurant,
        items,
        total: items.reduce((sum, entry) => sum + entry.item.price * entry.quantity, 0)
    };
}

document.querySelector('#cart-button').onclick = () => {
    const {restaurant, items, total} = cartDetails();
    if (!items.length) {
        toast('Your bag is waiting for something delicious.');
        return;
    }
    document.querySelector('#checkout-summary').textContent =
        `${items.reduce((sum, entry) => sum + entry.quantity, 0)} item(s) from ${restaurant.name} · ${money(total)}`;
    document.querySelector('#order-dialog').showModal();
};

document.querySelector('#order-form').onsubmit = async event => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const {items} = cartDetails();
    try {
        const customer = await api('/customers', {
            method: 'POST',
            body: JSON.stringify({
                name: form.get('customer'),
                email: form.get('email'),
                phone: form.get('phone')
            })
        });
        localStorage.setItem('foodCustomerId', customer.id);
        const order = await api('/orders', {
            method: 'POST',
            body: JSON.stringify({
                customerId: customer.id,
                customer: customer.name,
                address: form.get('address'),
                restaurantId: selectedRestaurant,
                items: items.map(entry => ({id: entry.id, quantity: entry.quantity}))
            })
        });
        document.querySelector('#order-dialog').close();
        event.currentTarget.reset();
        cart = {};
        selectedRestaurant = null;
        document.querySelector('#cart-count').textContent = '0';
        toast(`Order ${order.id} is in!`);
        await Promise.all([loadRestaurants(), loadOrders()]);
    } catch (error) {
        toast(error.message);
    }
};

document.querySelector('#filters').onclick = event => {
    const button = event.target.closest('.filter');
    if (!button) return;
    activeFilter = button.dataset.filter;
    document.querySelectorAll('.filter').forEach(filter => filter.classList.toggle('active', filter === button));
    renderRestaurants();
};

function renderOrders() {
    const element = document.querySelector('#orders-list');
    if (!orders.length) {
        element.innerHTML = '<p class="empty-orders">Your next favourite is only a few taps away.</p>';
        return;
    }
    element.innerHTML = [...orders].reverse().slice(0, 5).map(order => {
        const index = lifecycle.indexOf(order.status);
        return `<article class="order-row">
          <div><span class="order-name">${order.restaurantName} · ${money(order.total)}</span>
            <span class="order-sub">${order.id} · ${order.items.length} item(s) · ${order.address}</span></div>
          <div class="order-progress">${lifecycle.map((_, step) => `<i class="step ${index >= step ? 'done' : ''}"></i>`).join('')}</div>
          <div class="order-status"><span class="status-pill">${order.status.replaceAll('_', ' ')}</span>
            ${order.status !== 'DELIVERED' && order.status !== 'CANCELLED' ? `<button class="advance" data-id="${order.id}">Update status ↗</button>` : ''}
          </div>
        </article>`;
    }).join('');

    element.querySelectorAll('.advance').forEach(button => {
        button.onclick = async () => {
            const order = orders.find(item => item.id === button.dataset.id);
            const next = lifecycle[Math.min(lifecycle.indexOf(order.status) + 1, lifecycle.length - 1)];
            try {
                await api(`/orders/${encodeURIComponent(order.id)}/status`, {
                    method: 'PATCH', body: JSON.stringify({status: next})
                });
                await loadOrders();
            } catch (error) {
                toast(error.message);
                await loadOrders();
            }
        };
    });
}

async function loadOrders() {
    try {
        const customerId = localStorage.getItem('foodCustomerId');
        orders = customerId
            ? await api(`/customers/${encodeURIComponent(customerId)}/orders`)
            : await api('/orders');
        renderOrders();
    } catch {
        // The API can still be starting while the page is open.
    }
}

async function init() {
    try {
        await loadRestaurants();
        await loadOrders();
        setInterval(() => Promise.all([loadOrders(), loadRestaurants()]), 8000);
    } catch {
        grid.innerHTML = '<div class="error">The ordering service is not available. Start the project with Docker Compose, then refresh this page.</div>';
    }
}

init();
