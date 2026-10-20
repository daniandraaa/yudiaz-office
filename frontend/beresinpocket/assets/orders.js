'use strict';
(() => {
  const form = document.querySelector('#order-form');
  const message = document.querySelector('#order-message');
  const submit = document.querySelector('#order-submit');
  const panel = document.querySelector('#order-status');
  const planSelect = document.querySelector('#plan-select');
  const usersInput = document.querySelector('#users-input');
  const usersHint = document.querySelector('#users-hint');
  
  // Dynamic Elements in Panel
  const badgeStatus = document.querySelector('#badge-status');
  const orderIdText = document.querySelector('#order-id-text');
  const qrisMerchant = document.querySelector('#qris-merchant');
  const qrisNmid = document.querySelector('#qris-nmid');
  const qrisImg = document.querySelector('#qris-img');
  const summaryPlan = document.querySelector('#summary-plan');
  const summaryBase = document.querySelector('#summary-base');
  const summaryCode = document.querySelector('#summary-code');
  const summaryTotal = document.querySelector('#summary-total');
  const summaryExpiry = document.querySelector('#summary-expiry');
  const btnCopy = document.querySelector('#btn-copy');
  
  const paymentActionBox = document.querySelector('#payment-action-box');
  const senderNameInput = document.querySelector('#sender-name-input');
  const btnConfirmPayment = document.querySelector('#btn-confirm-payment');
  const paymentSuccessNotice = document.querySelector('#payment-success-notice');
  const noticeTelegram = document.querySelector('#notice-telegram');

  let access = null;
  let currentOrder = null;
  try { access = JSON.parse(sessionStorage.getItem('bip-order')); } catch (_) {}

  const formatRupiah = (num) => {
    return 'Rp ' + Number(num).toLocaleString('id-ID');
  };

  const syncPlanUI = () => {
    const isPersonal = planSelect.value === 'personal';
    usersInput.max = isPersonal ? '1' : '5';
    if (isPersonal) {
      usersInput.value = '1';
      usersHint.textContent = 'Paket Pribadi dikunci tepat untuk 1 akun pengguna Telegram.';
    } else {
      if (Number(usersInput.value) < 2) usersInput.value = '3';
      usersHint.textContent = 'Paket Organisasi mendukung 1 hingga 5 akun pengurus inti.';
    }

    document.querySelectorAll('.pricing-card').forEach(c => c.classList.remove('selected'));
    const activeCard = document.querySelector(isPersonal ? '#card-personal' : '#card-organization');
    if (activeCard) activeCard.classList.add('selected');
  };

  // Plan selection buttons
  document.querySelectorAll('.btn-select-plan').forEach(btn => {
    btn.addEventListener('click', () => {
      const targetPlan = btn.dataset.plan;
      planSelect.value = targetPlan;
      syncPlanUI();
      form.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });

  planSelect.addEventListener('change', syncPlanUI);

  const displayOrder = (order) => {
    currentOrder = order;
    panel.hidden = false;
    orderIdText.textContent = order.id;
    qrisMerchant.textContent = order.merchant_name || 'BeresInPocket, Elektronik';
    qrisNmid.textContent = order.nmid ? `NMID: ${order.nmid}` : 'NMID: ID1026620533368';
    if (order.qris_image_url) {
      qrisImg.src = order.qris_image_url;
    }

    const planName = order.plan === 'personal' ? 'Paket Pribadi (1 Akun)' : `Paket Organisasi (${order.users} Akun)`;
    summaryPlan.textContent = planName;
    summaryBase.textContent = formatRupiah(order.base_idr);
    summaryCode.textContent = `+Rp ${order.unique_code}`;
    summaryTotal.textContent = formatRupiah(order.total_idr);

    if (order.expires_at) {
      const expDate = new Date(order.expires_at * 1000);
      summaryExpiry.textContent = expDate.toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit' }) + ' WIB (' + expDate.toLocaleDateString('id-ID') + ')';
    }

    noticeTelegram.textContent = order.telegram;

    // Badge status handling
    if (order.payment_status === 'submitted_for_verification') {
      badgeStatus.textContent = 'MENUNGGU VERIFIKASI FINANCE';
      badgeStatus.className = 'payment-badge-status submitted';
      paymentActionBox.hidden = true;
      paymentSuccessNotice.hidden = false;
    } else if (order.payment_status === 'verified') {
      badgeStatus.textContent = 'PEMBAYARAN TERVERIFIKASI ✓';
      badgeStatus.className = 'payment-badge-status verified';
      paymentActionBox.hidden = true;
      paymentSuccessNotice.hidden = false;
    } else {
      badgeStatus.textContent = 'MENUNGGU PEMBAYARAN QRIS';
      badgeStatus.className = 'payment-badge-status';
      paymentActionBox.hidden = false;
      paymentSuccessNotice.hidden = true;
    }

    panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  };

  // Copy total amount button
  btnCopy.addEventListener('click', async () => {
    if (!currentOrder) return;
    try {
      await navigator.clipboard.writeText(currentOrder.total_idr.toString());
      btnCopy.textContent = '✓ Nominal Tersalin!';
      setTimeout(() => { btnCopy.textContent = '📋 Salin Nominal Persis'; }, 2500);
    } catch (_) {
      alert(`Silakan salin nominal berikut: ${currentOrder.total_idr}`);
    }
  });

  // Submit payment confirmation
  btnConfirmPayment.addEventListener('click', async () => {
    if (!access || !access.id) return;
    btnConfirmPayment.disabled = true;
    btnConfirmPayment.textContent = 'Mengirim konfirmasi…';
    try {
      const senderVal = senderNameInput.value.trim();
      const res = await fetch(`api/orders/${encodeURIComponent(access.id)}/submit-payment`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${access.access_token}`
        },
        body: JSON.stringify({ sender_name: senderVal, sender_bank: '', notes: '' }),
        cache: 'no-store'
      });
      if (!res.ok) throw new Error();
      const updated = await res.json();
      displayOrder(updated);
      message.textContent = 'Konfirmasi pembayaran berhasil dicatat. Tim finance sedang memverifikasi mutasi Anda.';
    } catch (_) {
      alert('Gagal mengirim konfirmasi. Periksa koneksi internet Anda atau coba lagi.');
    } finally {
      btnConfirmPayment.disabled = false;
      btnConfirmPayment.textContent = '✓ Saya Sudah Bayar via QRIS';
    }
  });

  const refresh = async () => {
    if (!access) return;
    try {
      const response = await fetch(`api/orders/${encodeURIComponent(access.id)}`, {
        headers: { Authorization: `Bearer ${access.access_token}` },
        cache: 'no-store'
      });
      if (!response.ok) throw new Error();
      const order = await response.json();
      displayOrder(order);
      message.textContent = 'Status pesanan berhasil diperbarui.';
    } catch (_) {
      message.textContent = 'Belum dapat memeriksa status pesanan saat ini.';
    }
  };

  document.querySelector('#order-refresh').addEventListener('click', refresh);
  document.querySelector('#order-forget').addEventListener('click', () => {
    if (confirm('Apakah Anda ingin keluar dari rincian pesanan ini di perangkat ini?')) {
      access = null;
      currentOrder = null;
      try { sessionStorage.removeItem('bip-order'); } catch (_) {}
      panel.hidden = true;
      form.reset();
      syncPlanUI();
      message.textContent = 'Sesi pesanan telah ditutup di browser ini.';
    }
  });

  // Create Order Submission
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    submit.disabled = true;
    submit.textContent = 'Menerbitkan QRIS…';
    message.textContent = 'Sedang membuat kode pembayaran unik Anda…';

    try {
      const payload = {
        name: form.elements.name.value.trim(),
        telegram: form.elements.telegram.value.trim(),
        plan: form.elements.plan.value,
        users: Number(form.elements.users.value),
        needs: form.elements.needs.value.trim(),
        consent: form.elements.consent.checked
      };

      const response = await fetch('api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || 'Gagal membuat pesanan');
      }

      const order = await response.json();
      access = { id: order.id, access_token: order.access_token };
      try { sessionStorage.setItem('bip-order', JSON.stringify(access)); } catch (_) {}
      displayOrder(order);
      message.textContent = 'Pesanan dibuat! Silakan lakukan pembayaran QRIS sesuai nominal di atas.';
    } catch (err) {
      message.textContent = 'Terjadi kendala saat memproses pesanan: ' + err.message;
    } finally {
      submit.disabled = false;
      submit.innerHTML = '<span>Lanjut ke Pembayaran QRIS</span> <span aria-hidden="true">→</span>';
    }
  });

  // Health check on load
  fetch('api/health', { cache: 'no-store' }).then(async response => {
    if (!response.ok) throw new Error();
    const data = await response.json();
    if (data.payment_enabled) {
      submit.disabled = false;
      syncPlanUI();
      if (access) await refresh();
    }
  }).catch(() => {
    message.textContent = 'Layanan pemesanan sedang sinkronisasi. Silakan refresh halaman beberapa saat lagi.';
  });
})();
