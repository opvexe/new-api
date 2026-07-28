/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import {
  CHANNEL_FORM_DEFAULT_VALUES,
  channelFormSchema,
  transformFormDataToCreatePayload,
} from '../channel-form'

function createCloudflareForm(mode: 'rest' | 'byok', other: string) {
  return {
    ...CHANNEL_FORM_DEFAULT_VALUES,
    name: 'Cloudflare',
    type: 39,
    key: 'channel-key',
    models: 'anthropic/claude-opus-5',
    cloudflare_api_mode: mode,
    other,
  }
}

describe('Cloudflare channel validation', () => {
  test('REST mode accepts exactly one account ID', () => {
    const valid = channelFormSchema.safeParse(
      createCloudflareForm('rest', 'account-id')
    )
    const invalid = channelFormSchema.safeParse(
      createCloudflareForm('rest', 'account-id/gateway-id')
    )

    assert.equal(valid.success, true)
    assert.equal(invalid.success, false)
  })

  test('BYOK mode requires an account ID and gateway ID pair', () => {
    const valid = channelFormSchema.safeParse(
      createCloudflareForm('byok', 'account-id/default')
    )

    assert.equal(valid.success, true)
    for (const other of [
      '',
      'account-id',
      '/default',
      'account-id/',
      'account-id/default/extra',
    ]) {
      const invalid = channelFormSchema.safeParse(
        createCloudflareForm('byok', other)
      )
      assert.equal(invalid.success, false, `expected ${other} to be rejected`)
    }
  })

  test('both modes reject whitespace in identifiers', () => {
    const rest = channelFormSchema.safeParse(
      createCloudflareForm('rest', 'account id')
    )
    const byok = channelFormSchema.safeParse(
      createCloudflareForm('byok', 'account-id/default gateway')
    )

    assert.equal(rest.success, false)
    assert.equal(byok.success, false)
  })

  test('persists the selected API mode in channel settings', () => {
    const payload = transformFormDataToCreatePayload(
      createCloudflareForm('byok', 'account-id/default')
    )
    const settings = JSON.parse(String(payload.channel.settings))

    assert.equal(settings.cloudflare_api_mode, 'byok')
  })
})
