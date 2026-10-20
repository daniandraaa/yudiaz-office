'use strict';
(() => {
  const form = document.querySelector('#order-form');
  const message = document.querySelector('#order-message');
  const submit = document.querySelector('#order-submit');
  const panel = document.querySelector('#order-status');
  const detail = document.querySelector('#order-detail');
  let access = null;
  try { access = JSON.parse(sessionStorage.getItem('bip-order')); } catch (_) {}
  const describe = order => {
    panel.hidden = false;
    detail.textContent = `Nomor permintaan: ${order.id}. ${order.base_idr === null ? 'Menunggu konfirmasi penawaran admin. Harga belum ditetapkan.' : 'Penawaran admin tersedia.'} Pembayaran: belum dibayar, menunggu instruksi admin. Akses layanan belum aktif.`;
    if (order.base_idr !== null) detail.textContent += ` Harga dasar: Rp${order.base_idr}. Kode unik: +${order.unique_code}. Total persis: Rp${order.total_idr}. Berlaku hingga ${new Date(order.expires_at * 1000).toLocaleString('id-ID')}. ${order.quote_expired ? 'Kedaluwarsa. Jangan transfer; pembayaran terlambat perlu tinjauan manual.' : 'Reservasi 24 jam.'} QRIS belum tersedia; pembayaran tetap dinonaktifkan. Kode unik adalah tambahan, bukan potongan. QRIS statis memerlukan input total oleh pelanggan, tanpa pencocokan bank otomatis.`;
  };
  const refresh = async () => {
    if (!access) return;
    try {
      const response = await fetch(`api/orders/${encodeURIComponent(access.id)}`, {headers: {Authorization: `Bearer ${access.access_token}`}, cache: 'no-store'});
      if (!response.ok) throw new Error();
      describe(await response.json());
      message.textContent = 'Status diperbarui. Simpan nomor permintaan; akses status tersimpan hanya selama sesi tab ini.';
    } catch (_) { message.textContent = 'Status belum dapat diperiksa. Simpan nomor permintaan dan coba lagi nanti.'; }
  };
  form.elements.plan.addEventListener('change', () => {
    form.elements.users.max = form.elements.plan.value === 'personal' ? '1' : '5';
    if (form.elements.plan.value === 'personal') form.elements.users.value = '1';
  });
  document.querySelector('#order-refresh').addEventListener('click', refresh);
  document.querySelector('#order-forget').addEventListener('click', () => {
    access = null;
    try { sessionStorage.removeItem('bip-order'); } catch (_) {}
    panel.hidden = true;
    message.textContent = 'Akses status di perangkat dihapus. Ini tidak menghapus permintaan yang sudah dikirim; hubungi akun Instagram resmi untuk permintaan penghapusan data.';
  });
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    submit.disabled = true;
    message.textContent = 'Mengirim permintaan…';
    try {
      const response = await fetch('api/orders', {method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({name:form.elements.name.value, telegram:form.elements.telegram.value, plan:form.elements.plan.value, users:Number(form.elements.users.value), needs:form.elements.needs.value, consent:form.elements.consent.checked})});
      if (!response.ok) throw new Error();
      const order = await response.json();
      access = {id:order.id, access_token:order.access_token};
      try { sessionStorage.setItem('bip-order', JSON.stringify(access)); } catch (_) {}
      describe(order);
      message.textContent = 'Permintaan tersimpan. Belum ada tagihan atau pembayaran. Admin perlu mengonfirmasi harga dan ketersediaan; tidak ada aktivasi otomatis.';
      form.reset();
    } catch (_) {
      message.textContent = 'Permintaan belum dapat dikonfirmasi. Periksa status jika tersedia sebelum mencoba lagi. Tidak ada pembayaran yang diproses.';
    } finally { submit.disabled = false; }
  });
  fetch('api/health', {cache:'no-store'}).then(async response => {
    if (!response.ok || (await response.json()).mode !== 'admin_confirmed_quote') throw new Error();
    submit.disabled = false;
    message.textContent = 'Formulir siap untuk permintaan penawaran, bukan pembayaran.';
    if (access) await refresh();
  }).catch(() => { message.textContent = 'Formulir penawaran belum tersedia pada alamat ini. Tidak ada data yang dikirim. Ikuti @beresinpocket untuk kabar resmi.'; });
})();
