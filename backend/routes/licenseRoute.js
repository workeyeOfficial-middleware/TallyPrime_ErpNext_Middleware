// routes/licenseRoute.js  — add this to your Express app
// Usage: app.use('/api', require('./routes/licenseRoute'));

const express   = require('express');
const router    = express.Router();
const { validateLicense } = require('../services/lmsService'); // your existing LMS service

/**
 * POST /api/license/verify-email
 * Body: { email }
 * Returns: { valid, plan, endDate, customerEmail }
 */
router.post('/license/verify-email', async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ valid: false, reason: 'Email required' });

  try {
    const result = await validateLicense(email.trim().toLowerCase());
    // Only send what the frontend needs — never expose licenseId/keys to browser
    res.json({
      valid        : result.valid,
      plan         : result.plan,
      endDate      : result.endDate,
      customerEmail: result.customerEmail,
      reason       : result.reason || null,
    });
  } catch (e) {
    res.status(500).json({ valid: false, reason: e.message });
  }
});

module.exports = router;